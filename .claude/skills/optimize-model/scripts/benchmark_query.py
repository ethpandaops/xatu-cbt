#!/usr/bin/env python3
"""Benchmark a rendered ClickHouse SELECT query with cache-aware controls.

This script avoids writing data by wrapping the input query as a subquery and
streaming output to FORMAT Null. It also pulls execution metrics from
system.query_log using query_id when available.
"""

from __future__ import annotations

import argparse
import base64
import json
import statistics
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Dict, List, Optional


class BenchmarkError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--query", help="SQL query string")
    src.add_argument("--query-file", help="Path to SQL query file")

    parser.add_argument("--endpoint", default="http://chendpoint-xatu-cbt-clickhouse.analytics.production.ethpandaops:8123")
    parser.add_argument("--database", default="mainnet")
    parser.add_argument("--username", default="")
    parser.add_argument("--password", default="")

    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--skip-explain", action="store_true")
    parser.add_argument("--output", help="Write JSON output to file")
    return parser.parse_args()


def load_query(args: argparse.Namespace) -> str:
    if args.query:
        return args.query.strip()
    with open(args.query_file, "r", encoding="utf-8") as file:
        return file.read().strip()


def run_sql(
    endpoint: str,
    database: str,
    username: str,
    password: str,
    query: str,
    timeout: int,
    query_id: Optional[str] = None,
) -> str:
    params: Dict[str, str] = {
        "database": database,
        "wait_end_of_query": "1",
    }
    if query_id:
        params["query_id"] = query_id

    url = f"{endpoint.rstrip('/')}/?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, data=query.encode("utf-8"), method="POST")

    if username:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise BenchmarkError(f"HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise BenchmarkError(str(exc)) from exc
    except TimeoutError as exc:
        raise BenchmarkError(f"Request timed out: {exc}") from exc
    except socket.timeout as exc:
        raise BenchmarkError(f"Request timed out: {exc}") from exc


def get_query_log_columns(args: argparse.Namespace) -> List[str]:
    sql = "DESCRIBE TABLE system.query_log FORMAT JSON"
    try:
        raw = run_sql(
            args.endpoint,
            args.database,
            args.username,
            args.password,
            sql,
            timeout=min(args.timeout, 20),
        )
        parsed = json.loads(raw)
        return [row["name"] for row in parsed.get("data", []) if "name" in row]
    except Exception:  # noqa: BLE001
        return []


def fetch_query_log_row(
    args: argparse.Namespace,
    query_id: str,
    metrics: List[str],
) -> Optional[Dict[str, object]]:
    if not metrics:
        return None

    metric_sql = ", ".join(metrics)
    sql = (
        f"SELECT {metric_sql} "
        "FROM system.query_log "
        f"WHERE query_id = '{query_id}' "
        "AND type = 'QueryFinish' "
        "ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT JSON"
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            raw = run_sql(
                args.endpoint,
                args.database,
                args.username,
                args.password,
                sql,
                timeout=min(args.timeout, 20),
            )
            data = json.loads(raw).get("data", [])
            if data:
                return data[0]
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.5)
    return None


def explain_query(args: argparse.Namespace, query: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    statements = {
        "estimate": f"EXPLAIN ESTIMATE SELECT * FROM ({query})",
        "indexes": f"EXPLAIN indexes = 1 SELECT * FROM ({query})",
    }
    for key, statement in statements.items():
        try:
            result[key] = run_sql(
                args.endpoint,
                args.database,
                args.username,
                args.password,
                statement,
                timeout=min(args.timeout, 60),
            ).strip()
        except Exception as exc:  # noqa: BLE001
            result[key] = f"ERROR: {exc}"
    return result


def make_benchmark_sql(query: str) -> str:
    normalized = query.rstrip().rstrip(";")
    return (
        "SELECT * FROM ("
        + normalized
        + ") AS benchmark_query FORMAT Null SETTINGS "
        "use_query_cache = 0, use_query_condition_cache = 0"
    )


def summarize_runs(runs: List[Dict[str, object]]) -> Dict[str, object]:
    wall_times = [float(run["wall_time_s"]) for run in runs]
    summary: Dict[str, object] = {
        "run_count": len(runs),
        "wall_time_s_min": min(wall_times),
        "wall_time_s_median": statistics.median(wall_times),
        "wall_time_s_max": max(wall_times),
    }

    numeric_keys = ["read_rows", "read_bytes", "peak_memory_usage", "memory_usage"]

    def coerce_numeric(value: object) -> Optional[float]:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return None
        return None

    for key in numeric_keys:
        values = [coerce_numeric(run.get(key)) for run in runs]
        values = [v for v in values if v is not None]
        if values:
            summary[f"{key}_median"] = statistics.median(values)
            summary[f"{key}_max"] = max(values)

    return summary


def main() -> int:
    args = parse_args()
    query = load_query(args)

    if not query:
        raise BenchmarkError("Empty query input")

    benchmark_sql = make_benchmark_sql(query)

    # Determine which metrics exist in this ClickHouse version.
    available_cols = get_query_log_columns(args)
    preferred = [
        "query_duration_ms",
        "read_rows",
        "read_bytes",
        "result_rows",
        "result_bytes",
        "peak_memory_usage",
        "memory_usage",
    ]
    metrics = [col for col in preferred if col in available_cols]

    if not metrics and "query_duration_ms" in available_cols:
        metrics = ["query_duration_ms"]

    # Warmup runs are intentionally discarded.
    for _ in range(args.warmup):
        warmup_id = f"skill-warmup-{uuid.uuid4()}"
        run_sql(
            args.endpoint,
            args.database,
            args.username,
            args.password,
            benchmark_sql,
            timeout=args.timeout,
            query_id=warmup_id,
        )

    measured: List[Dict[str, object]] = []
    caveats: List[str] = []
    for idx in range(args.runs):
        run_id = f"skill-bench-{idx + 1}-{uuid.uuid4()}"
        started = time.perf_counter()
        run_sql(
            args.endpoint,
            args.database,
            args.username,
            args.password,
            benchmark_sql,
            timeout=args.timeout,
            query_id=run_id,
        )
        elapsed = time.perf_counter() - started

        row = fetch_query_log_row(args, run_id, metrics) or {}
        row["query_id"] = run_id
        row["wall_time_s"] = elapsed
        if not row:
            caveats.append(
                "Missing system.query_log metrics for at least one run; using wall time only."
            )
        measured.append(row)

    explain = {} if args.skip_explain else explain_query(args, query)

    payload = {
        "endpoint": args.endpoint,
        "database": args.database,
        "benchmark_sql": benchmark_sql,
        "cache_controls": [
            "use_query_cache = 0",
            "use_query_condition_cache = 0",
            "filesystem/page cache may still affect results",
        ],
        "metrics_collected": metrics,
        "runs": measured,
        "summary": summarize_runs(measured),
        "caveats": sorted(set(caveats)),
        "explain": explain,
    }

    serialized = json.dumps(payload, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as file:
            file.write(serialized + "\n")
    print(serialized)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BenchmarkError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
