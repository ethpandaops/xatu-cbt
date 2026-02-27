#!/usr/bin/env python3
"""Compare baseline vs candidate query output with exact-equality checks.

Method order:
1. Detect server version.
2. For ClickHouse >= 25.8, try `FORMAT Hash`.
3. For ClickHouse < 25.8 (or on `UNKNOWN_FORMAT`), use deterministic binary hashing:
   - `SELECT * FROM (...) ORDER BY tuple(*) FORMAT RowBinaryWithNamesAndTypes`
   - SHA256 of full byte stream

Exit codes:
- 0: equal
- 3: not equal
- 1: comparison failed
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional


class HashCompareError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-query-file", required=True)
    parser.add_argument("--candidate-query-file", required=True)
    parser.add_argument("--endpoint", default="http://chendpoint-xatu-cbt-clickhouse.analytics.production.ethpandaops:8123")
    parser.add_argument("--database", default="mainnet")
    parser.add_argument("--username", default="")
    parser.add_argument("--password", default="")
    parser.add_argument(
        "--server-version",
        default="",
        help="Optional ClickHouse version string. If omitted, script probes via SELECT version().",
    )
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--output", help="Write JSON output to file")
    return parser.parse_args()


def load_query(path: str) -> str:
    with open(path, "r", encoding="utf-8") as file:
        query = file.read().strip()
    if not query:
        raise HashCompareError(f"Query file is empty: {path}")
    return query


def build_request(
    endpoint: str,
    database: str,
    username: str,
    password: str,
    query: str,
) -> urllib.request.Request:
    params = urllib.parse.urlencode({"database": database, "wait_end_of_query": "1"})
    url = f"{endpoint.rstrip('/')}/?{params}"
    request = urllib.request.Request(url, data=query.encode("utf-8"), method="POST")
    if username:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")
    return request


def run_sql_text(
    endpoint: str,
    database: str,
    username: str,
    password: str,
    query: str,
    timeout: int,
) -> str:
    request = build_request(endpoint, database, username, password, query)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise HashCompareError(f"HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise HashCompareError(str(exc)) from exc
    except TimeoutError as exc:
        raise HashCompareError(f"Request timed out: {exc}") from exc
    except socket.timeout as exc:
        raise HashCompareError(f"Request timed out: {exc}") from exc


def stream_sql_sha256(
    endpoint: str,
    database: str,
    username: str,
    password: str,
    query: str,
    timeout: int,
) -> str:
    request = build_request(endpoint, database, username, password, query)
    digest = hashlib.sha256()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            return digest.hexdigest()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise HashCompareError(f"HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise HashCompareError(str(exc)) from exc
    except TimeoutError as exc:
        raise HashCompareError(f"Request timed out: {exc}") from exc
    except socket.timeout as exc:
        raise HashCompareError(f"Request timed out: {exc}") from exc


def make_hash_sql(query: str) -> str:
    normalized = query.rstrip().rstrip(";")
    return (
        "SELECT * FROM ("
        + normalized
        + ") AS hash_query SETTINGS use_query_cache = 0, use_query_condition_cache = 0 FORMAT Hash"
    )


def make_rowbinary_hash_sql(query: str) -> str:
    normalized = query.rstrip().rstrip(";")
    return (
        "SELECT * FROM ("
        + normalized
        + ") AS hash_query ORDER BY tuple(*) SETTINGS "
        "use_query_cache = 0, use_query_condition_cache = 0 FORMAT RowBinaryWithNamesAndTypes"
    )


def make_count_sql(query: str) -> str:
    normalized = query.rstrip().rstrip(";")
    return (
        "SELECT count() FROM ("
        + normalized
        + ") AS hash_query SETTINGS use_query_cache = 0, use_query_condition_cache = 0 FORMAT TabSeparatedRaw"
    )


def parse_int(raw: str) -> int:
    value = raw.strip().splitlines()[0].strip() if raw.strip() else ""
    return int(value)


def parse_major_minor(version: str) -> tuple[int, int] | None:
    match = re.match(r"^\s*(\d+)\.(\d+)", version.strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def should_attempt_format_hash(version: str) -> bool:
    parsed = parse_major_minor(version)
    if parsed is None:
        # If version parsing fails, preserve existing behavior and attempt FORMAT Hash.
        return True
    return parsed >= (25, 8)


def resolve_server_version(args: argparse.Namespace) -> str:
    if args.server_version.strip():
        return args.server_version.strip()
    raw = run_sql_text(
        args.endpoint,
        args.database,
        args.username,
        args.password,
        "SELECT version() FORMAT TabSeparatedRaw",
        args.timeout,
    ).strip()
    return raw.splitlines()[0].strip() if raw else ""


def write_output(payload: dict, output_path: Optional[str]) -> None:
    text = json.dumps(payload, indent=2)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as file:
            file.write(text + "\n")
    print(text)


def compare_with_format_hash(args: argparse.Namespace, baseline_query: str, candidate_query: str) -> dict:
    baseline_sql = make_hash_sql(baseline_query)
    candidate_sql = make_hash_sql(candidate_query)

    baseline_raw = run_sql_text(
        args.endpoint,
        args.database,
        args.username,
        args.password,
        baseline_sql,
        args.timeout,
    ).strip()
    candidate_raw = run_sql_text(
        args.endpoint,
        args.database,
        args.username,
        args.password,
        candidate_sql,
        args.timeout,
    ).strip()

    return {
        "method": "format_hash",
        "baseline_hash_raw": baseline_raw,
        "candidate_hash_raw": candidate_raw,
        "hashes_match": baseline_raw == candidate_raw,
    }


def compare_with_rowbinary_sha256(
    args: argparse.Namespace,
    baseline_query: str,
    candidate_query: str,
    fallback_reason: str,
) -> dict:
    baseline_count = parse_int(
        run_sql_text(
            args.endpoint,
            args.database,
            args.username,
            args.password,
            make_count_sql(baseline_query),
            args.timeout,
        )
    )
    candidate_count = parse_int(
        run_sql_text(
            args.endpoint,
            args.database,
            args.username,
            args.password,
            make_count_sql(candidate_query),
            args.timeout,
        )
    )

    baseline_sha256 = stream_sql_sha256(
        args.endpoint,
        args.database,
        args.username,
        args.password,
        make_rowbinary_hash_sql(baseline_query),
        args.timeout,
    )
    candidate_sha256 = stream_sql_sha256(
        args.endpoint,
        args.database,
        args.username,
        args.password,
        make_rowbinary_hash_sql(candidate_query),
        args.timeout,
    )

    hashes_match = baseline_count == candidate_count and baseline_sha256 == candidate_sha256
    return {
        "method": "rowbinary_sha256_fallback",
        "fallback_reason": fallback_reason,
        "order_applied": "ORDER BY tuple(*)",
        "baseline_count": baseline_count,
        "candidate_count": candidate_count,
        "baseline_sha256": baseline_sha256,
        "candidate_sha256": candidate_sha256,
        "hashes_match": hashes_match,
    }


def main() -> int:
    args = parse_args()
    baseline_query = load_query(args.baseline_query_file)
    candidate_query = load_query(args.candidate_query_file)

    payload = {
        "endpoint": args.endpoint,
        "database": args.database,
        "baseline_query_file": args.baseline_query_file,
        "candidate_query_file": args.candidate_query_file,
        "format_hash_min_version": "25.8",
    }

    server_version = ""
    try:
        server_version = resolve_server_version(args)
    except HashCompareError as exc:
        payload["version_probe_error"] = str(exc)
    payload["server_version"] = server_version

    if server_version and not should_attempt_format_hash(server_version):
        payload["format_hash_attempted"] = False
        try:
            comparison = compare_with_rowbinary_sha256(
                args,
                baseline_query,
                candidate_query,
                fallback_reason=f"server_version {server_version} < 25.8",
            )
            payload.update(comparison)
            write_output(payload, args.output)
            return 0 if payload["hashes_match"] else 3
        except HashCompareError as exc:
            payload["method"] = "rowbinary_sha256_fallback"
            payload["fallback_reason"] = f"server_version {server_version} < 25.8"
            payload["error"] = str(exc)
            write_output(payload, args.output)
            return 1

    try:
        payload["format_hash_attempted"] = True
        comparison = compare_with_format_hash(args, baseline_query, candidate_query)
        payload.update(comparison)
        write_output(payload, args.output)
        return 0 if payload["hashes_match"] else 3
    except HashCompareError as exc:
        error_text = str(exc)
        if "UNKNOWN_FORMAT" not in error_text or "Hash" not in error_text:
            payload["method"] = "format_hash"
            payload["error"] = error_text
            write_output(payload, args.output)
            return 1

        # Automatic exact fallback for older ClickHouse versions.
        try:
            comparison = compare_with_rowbinary_sha256(
                args, baseline_query, candidate_query, fallback_reason=error_text
            )
            payload.update(comparison)
            write_output(payload, args.output)
            return 0 if payload["hashes_match"] else 3
        except HashCompareError as fallback_exc:
            payload["method"] = "rowbinary_sha256_fallback"
            payload["fallback_reason"] = error_text
            payload["error"] = str(fallback_exc)
            write_output(payload, args.output)
            return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HashCompareError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
