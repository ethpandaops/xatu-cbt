#!/usr/bin/env python3
"""Fetch SHOW CREATE metadata for model dependencies across ClickHouse clusters.

Supports auto-introspection of Distributed tables by resolving and inspecting
underlying local tables when available.
"""

from __future__ import annotations

import argparse
import base64
import json
import pathlib
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional


class IntrospectionError(Exception):
    pass


def sql_quote(value: str) -> str:
    return value.replace("'", "''")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolve-json", help="JSON output from resolve_model_sql.py")
    parser.add_argument(
        "--dependency",
        action="append",
        default=[],
        help="Dependency in kind:table format (e.g. external:canonical_beacon_block)",
    )

    parser.add_argument("--external-endpoint", default="http://chendpoint-xatu-clickhouse.analytics.production.ethpandaops:8123")
    parser.add_argument("--external-database", default="default")
    parser.add_argument("--external-username", default="")
    parser.add_argument("--external-password", default="")

    parser.add_argument("--transformation-endpoint", default="http://chendpoint-xatu-cbt-clickhouse.analytics.production.ethpandaops:8123")
    parser.add_argument("--transformation-database", default="mainnet")
    parser.add_argument("--transformation-username", default="")
    parser.add_argument("--transformation-password", default="")

    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--output", help="Write JSON to file")
    return parser.parse_args()


def run_sql(
    endpoint: str,
    database: str,
    username: str,
    password: str,
    query: str,
    timeout: int,
) -> str:
    params = urllib.parse.urlencode({"database": database, "wait_end_of_query": "1"})
    url = f"{endpoint.rstrip('/')}/?{params}"
    request = urllib.request.Request(url, data=query.encode("utf-8"), method="POST")

    if username:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise IntrospectionError(f"HTTP {exc.code} querying {endpoint}: {body}") from exc
    except urllib.error.URLError as exc:
        raise IntrospectionError(f"Failed querying {endpoint}: {exc}") from exc


def parse_dependencies(args: argparse.Namespace) -> List[Dict[str, str]]:
    deps: List[Dict[str, str]] = []

    if args.resolve_json:
        data = json.loads(pathlib.Path(args.resolve_json).read_text(encoding="utf-8"))
        for dep in data.get("dependencies", []):
            kind = dep.get("kind", "unknown")
            table = dep.get("table", "")
            if kind in {"external", "transformation"} and table:
                deps.append({"kind": kind, "table": table})

    for raw in args.dependency:
        if ":" not in raw:
            raise IntrospectionError(f"Invalid dependency format '{raw}', expected kind:table")
        kind, table = raw.split(":", 1)
        kind = kind.strip()
        table = table.strip()
        if kind not in {"external", "transformation"}:
            raise IntrospectionError(f"Unsupported dependency kind '{kind}'")
        if not table:
            raise IntrospectionError("Empty table in dependency")
        deps.append({"kind": kind, "table": table})

    unique = {(dep["kind"], dep["table"]): dep for dep in deps}
    return [unique[key] for key in sorted(unique)]


def extract_single_line(pattern: str, sql: str) -> Optional[str]:
    match = re.search(pattern, sql, flags=re.IGNORECASE | re.MULTILINE)
    return match.group(1).strip() if match else None


def normalize_show_create_sql(sql: str) -> str:
    text = sql.strip()
    # HTTP TabSeparated output can escape newlines and quotes for single-string cells.
    if "\\n" in text and "\n" not in text:
        text = text.replace("\\n", "\n")
    text = text.replace("\\t", "\t")
    text = text.replace("\\'", "'")
    text = text.replace('\\"', '"')
    return text


def split_top_level_args(arg_str: str) -> List[str]:
    args: List[str] = []
    current: List[str] = []
    depth = 0
    in_quote = False
    quote_char = "'"

    for char in arg_str:
        if in_quote:
            current.append(char)
            if char == quote_char:
                in_quote = False
            continue

        if char in {"'", '"'}:
            in_quote = True
            quote_char = char
            current.append(char)
            continue

        if char == "(":
            depth += 1
            current.append(char)
            continue
        if char == ")":
            depth = max(0, depth - 1)
            current.append(char)
            continue

        if char == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
            continue

        current.append(char)

    if current:
        args.append("".join(current).strip())
    return args


def unquote(value: str) -> str:
    value = value.strip()
    if (value.startswith("'") and value.endswith("'")) or (
        value.startswith('"') and value.endswith('"')
    ):
        return value[1:-1]
    return value


def parse_distributed_target(engine: Optional[str]) -> Optional[Dict[str, str]]:
    if not engine:
        return None
    match = re.search(r"Distributed\s*\((.*)\)", engine, flags=re.IGNORECASE)
    if not match:
        return None
    args = split_top_level_args(match.group(1))
    if len(args) < 3:
        return None
    return {
        "cluster": unquote(args[0]),
        "database": unquote(args[1]),
        "table": unquote(args[2]),
    }


def parse_show_create(sql: str) -> Dict[str, Optional[str]]:
    normalized_sql = normalize_show_create_sql(sql)
    engine = extract_single_line(r"^\s*ENGINE\s*=\s*(.+)$", normalized_sql)
    primary_key = extract_single_line(r"^\s*PRIMARY KEY\s+(.+)$", normalized_sql)
    order_by = extract_single_line(r"^\s*ORDER BY\s+(.+)$", normalized_sql)
    parsed = {
        "engine": engine,
        "partition_by": extract_single_line(r"^\s*PARTITION BY\s+(.+)$", normalized_sql),
        "primary_key": primary_key,
        "order_by": order_by,
        "effective_primary_key": primary_key or order_by,
        "sample_by": extract_single_line(r"^\s*SAMPLE BY\s+(.+)$", normalized_sql),
    }
    return parsed


def fetch_show_create(
    endpoint: str,
    database: str,
    username: str,
    password: str,
    table: str,
    timeout: int,
) -> str:
    sql = f"SHOW CREATE TABLE `{database}`.`{table}`"
    return run_sql(endpoint, database, username, password, sql, timeout)


def fetch_system_table_metadata(
    endpoint: str,
    database: str,
    username: str,
    password: str,
    table: str,
    timeout: int,
) -> Optional[Dict[str, str]]:
    query = (
        "SELECT engine, engine_full, partition_key, sorting_key, primary_key "
        "FROM system.tables "
        f"WHERE database = '{sql_quote(database)}' AND name = '{sql_quote(table)}' "
        "LIMIT 1 FORMAT JSONEachRow"
    )
    raw = run_sql(endpoint, database, username, password, query, timeout).strip()
    if not raw:
        return None
    line = raw.splitlines()[0]
    data = json.loads(line)
    return {
        "engine": str(data.get("engine", "")).strip(),
        "engine_full": str(data.get("engine_full", "")).strip(),
        "partition_key": str(data.get("partition_key", "")).strip(),
        "sorting_key": str(data.get("sorting_key", "")).strip(),
        "primary_key": str(data.get("primary_key", "")).strip(),
    }


def merge_with_system_tables_fallback(
    parsed: Dict[str, Optional[str]],
    sys_meta: Optional[Dict[str, str]],
) -> Dict[str, Optional[str]]:
    if not sys_meta:
        return parsed

    merged = dict(parsed)
    if not merged.get("engine"):
        merged["engine"] = sys_meta.get("engine_full") or sys_meta.get("engine")
    if not merged.get("partition_by"):
        merged["partition_by"] = sys_meta.get("partition_key") or None
    if not merged.get("order_by"):
        merged["order_by"] = sys_meta.get("sorting_key") or None
    if not merged.get("primary_key"):
        merged["primary_key"] = sys_meta.get("primary_key") or None
    merged["effective_primary_key"] = merged.get("primary_key") or merged.get("order_by")
    return merged


def main() -> int:
    args = parse_args()
    deps = parse_dependencies(args)
    if not deps:
        raise IntrospectionError("No dependencies to introspect")

    cluster_cfg = {
        "external": {
            "endpoint": args.external_endpoint,
            "database": args.external_database,
            "username": args.external_username,
            "password": args.external_password,
        },
        "transformation": {
            "endpoint": args.transformation_endpoint,
            "database": args.transformation_database,
            "username": args.transformation_username,
            "password": args.transformation_password,
        },
    }

    results: List[Dict[str, object]] = []

    for dep in deps:
        kind = dep["kind"]
        table = dep["table"]
        cfg = cluster_cfg[kind]

        row: Dict[str, object] = {
            "kind": kind,
            "table": table,
            "database": cfg["database"],
            "endpoint": cfg["endpoint"],
        }

        try:
            create_sql = fetch_show_create(
                cfg["endpoint"],
                cfg["database"],
                cfg["username"],
                cfg["password"],
                table,
                args.timeout,
            )
            normalized_create = normalize_show_create_sql(create_sql)
            row["show_create"] = normalized_create.strip()
            parsed = parse_show_create(normalized_create)
            sys_meta = fetch_system_table_metadata(
                cfg["endpoint"],
                cfg["database"],
                cfg["username"],
                cfg["password"],
                table,
                args.timeout,
            )
            parsed = merge_with_system_tables_fallback(parsed, sys_meta)
            row.update(parsed)
            if sys_meta:
                row["system_tables"] = sys_meta

            distributed = parse_distributed_target(parsed.get("engine"))
            if distributed:
                row["distributed_target"] = distributed
                local_table = distributed["table"]
                local_db = distributed["database"] or cfg["database"]

                local_result: Dict[str, object] = {
                    "database": local_db,
                    "table": local_table,
                }
                try:
                    local_create = fetch_show_create(
                        cfg["endpoint"],
                        local_db,
                        cfg["username"],
                        cfg["password"],
                        local_table,
                        args.timeout,
                    )
                    normalized_local_create = normalize_show_create_sql(local_create)
                    local_result["show_create"] = normalized_local_create.strip()
                    local_parsed = parse_show_create(normalized_local_create)
                    local_sys_meta = fetch_system_table_metadata(
                        cfg["endpoint"],
                        local_db,
                        cfg["username"],
                        cfg["password"],
                        local_table,
                        args.timeout,
                    )
                    local_parsed = merge_with_system_tables_fallback(local_parsed, local_sys_meta)
                    local_result.update(local_parsed)
                    if local_sys_meta:
                        local_result["system_tables"] = local_sys_meta
                except Exception as local_exc:  # noqa: BLE001
                    local_result["error"] = str(local_exc)

                row["local_table"] = local_result
        except Exception as exc:  # noqa: BLE001
            row["error"] = str(exc)

        results.append(row)

    total = len(results)
    failed = sum(1 for row in results if "error" in row)
    distributed = sum(1 for row in results if "distributed_target" in row)
    local_failures = sum(
        1
        for row in results
        if isinstance(row.get("local_table"), dict)
        and isinstance(row["local_table"], dict)
        and "error" in row["local_table"]
    )

    payload = json.dumps(
        {
            "tables": results,
            "summary": {
                "total_tables": total,
                "successful_tables": total - failed,
                "failed_tables": failed,
                "distributed_tables": distributed,
                "local_table_failures": local_failures,
            },
        },
        indent=2,
    )
    if args.output:
        pathlib.Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except IntrospectionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
