#!/usr/bin/env python3
"""Run model resolution + schema introspection with one command.

Purpose: reduce CLI discovery so agents can call one stable entrypoint.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import List


class PrepError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, help="Transformation model name or path")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--out-dir", default="/tmp")
    parser.add_argument("--prefix", default="optimize-model")
    parser.add_argument(
        "--session-id",
        default="",
        help="Optional session id used to isolate temp artifact names.",
    )

    parser.add_argument(
        "--external-endpoint",
        default="http://chendpoint-xatu-clickhouse.analytics.production.ethpandaops:8123",
    )
    parser.add_argument("--external-database", default="default")
    parser.add_argument("--external-username", default="")
    parser.add_argument("--external-password", default="")

    parser.add_argument("--transformation-endpoint", default="http://chendpoint-xatu-cbt-clickhouse.analytics.production.ethpandaops:8123")
    parser.add_argument("--transformation-database", default="mainnet")
    parser.add_argument("--transformation-username", default="")
    parser.add_argument("--transformation-password", default="")

    parser.add_argument(
        "--access-to-external-cluster",
        default="cluster('{remote_cluster}', database.table_name)",
    )
    parser.add_argument("--network", default="mainnet")
    parser.add_argument("--bounds-start", type=int)
    parser.add_argument("--bounds-end", type=int)
    parser.add_argument("--task-start", type=int)
    parser.add_argument(
        "--period-mode",
        choices=["sane", "daring", "custom"],
        default="sane",
        help="How aggressively to size test bounds when bounds are not provided.",
    )
    parser.add_argument(
        "--expand-factor",
        type=float,
        default=2.0,
        help="Multiplier used for daring mode.",
    )
    parser.add_argument("--slot-seconds", type=int, default=12)
    parser.add_argument("--slot-cap-seconds-sane", type=int, default=6 * 60 * 60)
    parser.add_argument("--slot-cap-seconds-daring", type=int, default=24 * 60 * 60)
    parser.add_argument("--block-cap-sane", type=int, default=20000)
    parser.add_argument("--block-cap-daring", type=int, default=100000)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--output", help="Optional manifest output path")
    return parser.parse_args()


def run(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=False)


def run_sql_http(
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
        import base64

        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")

    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


def parse_frontmatter(content: str) -> tuple[str, str]:
    if not content.startswith("---\n"):
        return "", content
    marker = "\n---\n"
    idx = content.find(marker, 4)
    if idx == -1:
        return "", content
    return content[4:idx], content[idx + len(marker) :]


def normalize_model_path(repo_root: pathlib.Path, model_arg: str) -> pathlib.Path:
    raw = pathlib.Path(model_arg)
    candidates: List[pathlib.Path] = []

    if raw.is_absolute():
        candidates.append(raw)
    else:
        candidates.extend(
            [
                repo_root / raw,
                repo_root / "models" / "transformations" / raw,
                repo_root / "models" / "transformations" / f"{raw}.sql",
            ]
        )
    if raw.suffix == ".sql":
        candidates.append(repo_root / "models" / "transformations" / raw.name)

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise PrepError(f"Could not resolve model path for '{model_arg}'")


def parse_interval(frontmatter: str) -> dict:
    interval: dict = {"type": "slot", "max": 50000}
    in_interval = False
    for line in frontmatter.splitlines():
        if re.match(r"^\s*interval\s*:\s*$", line):
            in_interval = True
            continue
        if in_interval and re.match(r"^\S", line):
            break
        if not in_interval:
            continue
        type_match = re.match(r"^\s*type\s*:\s*([A-Za-z0-9_]+)\s*$", line)
        if type_match:
            interval["type"] = type_match.group(1).strip()
        max_match = re.match(r"^\s*max\s*:\s*([0-9]+)\s*$", line)
        if max_match:
            interval["max"] = int(max_match.group(1))
    return interval


def parse_int_from_sql_result(raw: str) -> int | None:
    value = raw.strip().splitlines()[0].strip() if raw.strip() else ""
    if not value or value.lower() == "null":
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def resolve_head_block(args: argparse.Namespace) -> tuple[int | None, str | None]:
    candidates = [
        (
            "transformation:int_execution_block_by_date",
            args.transformation_endpoint,
            args.transformation_database,
            args.transformation_username,
            args.transformation_password,
            f"SELECT max(block_number) FROM `{args.transformation_database}`.`int_execution_block_by_date` FORMAT TabSeparatedRaw",
        ),
        (
            "external:canonical_execution_block",
            args.external_endpoint,
            args.external_database,
            args.external_username,
            args.external_password,
            (
                f"SELECT max(block_number) FROM `{args.external_database}`.`canonical_execution_block` "
                f"WHERE meta_network_name = '{args.network}' FORMAT TabSeparatedRaw"
            ),
        ),
    ]
    for label, endpoint, database, username, password, sql in candidates:
        try:
            raw = run_sql_http(endpoint, database, username, password, sql, args.timeout)
            parsed = parse_int_from_sql_result(raw)
            if parsed is not None:
                return parsed, label
        except Exception:  # noqa: BLE001
            continue
    return None, None


def parse_major_minor(version: str) -> str | None:
    match = re.match(r"^\s*(\d+)\.(\d+)", version.strip())
    if not match:
        return None
    return f"{match.group(1)}.{match.group(2)}"


def release_blog_url(version: str) -> str | None:
    major_minor = parse_major_minor(version)
    if not major_minor:
        return None
    major, minor = major_minor.split(".")
    return f"https://clickhouse.com/blog/clickhouse-release-{major}-{int(minor):02d}"


def resolve_clickhouse_versions(args: argparse.Namespace) -> dict:
    clusters = {
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

    results: dict = {}
    for cluster_name, cfg in clusters.items():
        entry = {
            "endpoint": cfg["endpoint"],
            "database": cfg["database"],
        }
        try:
            version_raw = run_sql_http(
                cfg["endpoint"],
                cfg["database"],
                cfg["username"],
                cfg["password"],
                "SELECT version() FORMAT TabSeparatedRaw",
                args.timeout,
            ).strip()
            version = version_raw.splitlines()[0].strip() if version_raw else ""
            entry["version"] = version
            entry["major_minor"] = parse_major_minor(version)

            has_hash_raw = run_sql_http(
                cfg["endpoint"],
                cfg["database"],
                cfg["username"],
                cfg["password"],
                "SELECT countIf(name='Hash') FROM system.formats FORMAT TabSeparatedRaw",
                args.timeout,
            ).strip()
            has_hash = parse_int_from_sql_result(has_hash_raw) or 0
            entry["supports_format_hash"] = has_hash > 0

            doc_links = {
                "formats_hash": "https://clickhouse.com/docs/interfaces/formats/Hash",
                "query_optimization": "https://clickhouse.com/docs/optimize/query-optimization",
            }
            release_link = release_blog_url(version)
            if release_link:
                doc_links["release_blog_for_version"] = release_link
            entry["docs"] = doc_links
        except Exception as exc:  # noqa: BLE001
            entry["error"] = str(exc)
        results[cluster_name] = entry
    return results


def compute_bounds(
    args: argparse.Namespace,
    interval_type: str,
    interval_max: int,
) -> tuple[int, int, dict]:
    now_ts = int(dt.datetime.now(dt.timezone.utc).timestamp())
    mode = args.period_mode
    if args.bounds_start is not None and args.bounds_end is not None:
        return args.bounds_start, args.bounds_end, {
            "source": "user",
            "mode": "custom",
            "unit": "timestamp" if interval_type == "slot" else "block",
        }

    if mode == "custom":
        raise PrepError("period-mode=custom requires both --bounds-start and --bounds-end")

    if interval_type == "slot":
        base_seconds = max(args.slot_seconds, interval_max * args.slot_seconds)
        if mode == "daring":
            window = int(base_seconds * max(1.0, args.expand_factor))
            window = min(window, args.slot_cap_seconds_daring)
        else:
            window = min(base_seconds, args.slot_cap_seconds_sane)
        end = now_ts
        start = max(0, end - max(args.slot_seconds, window))
        return start, end, {
            "source": "frontmatter_interval",
            "mode": mode,
            "unit": "timestamp",
            "interval_type": interval_type,
            "interval_max": interval_max,
            "window_size": end - start,
        }

    if interval_type == "block":
        base_blocks = max(1, interval_max)
        if mode == "daring":
            window = int(base_blocks * max(1.0, args.expand_factor))
            window = min(window, args.block_cap_daring)
        else:
            window = min(base_blocks, args.block_cap_sane)

        head_block, head_source = resolve_head_block(args)
        if head_block is None:
            head_block = window
            head_source = "fallback:interval_max"
        end = max(1, head_block)
        start = max(0, end - window)
        return start, end, {
            "source": "frontmatter_interval",
            "mode": mode,
            "unit": "block",
            "interval_type": interval_type,
            "interval_max": interval_max,
            "window_size": end - start,
            "head_block": end,
            "head_block_source": head_source,
        }

    raise PrepError(f"Unsupported interval type '{interval_type}'. Expected slot or block.")


def require_file(path: pathlib.Path, label: str) -> None:
    if not path.is_file():
        raise PrepError(f"Expected {label} at {path}, but file was not created")


def load_json(path: pathlib.Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PrepError(f"Invalid JSON in {path}: {exc}") from exc


def main() -> int:
    args = parse_args()
    repo_root = pathlib.Path(args.repo_root).resolve()
    model_path = normalize_model_path(repo_root, args.model)
    model_content = model_path.read_text(encoding="utf-8")
    frontmatter, _ = parse_frontmatter(model_content)
    interval = parse_interval(frontmatter)
    interval_type = str(interval.get("type", "slot")).strip()
    interval_max = int(interval.get("max", 50000))
    bounds_start, bounds_end, period_info = compute_bounds(args, interval_type, interval_max)
    task_start = args.task_start if args.task_start is not None else int(
        dt.datetime.now(dt.timezone.utc).timestamp()
    )

    script_dir = pathlib.Path(__file__).resolve().parent
    resolve_script = script_dir / "resolve_model_sql.py"
    introspect_script = script_dir / "introspect_tables.py"

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    session_id = args.session_id.strip() or uuid.uuid4().hex[:8]
    artifact_prefix = f"{args.prefix}.{session_id}"

    resolve_json = out_dir / f"{artifact_prefix}.resolve.json"
    schema_json = out_dir / f"{artifact_prefix}.schemas.json"
    rendered_sql = out_dir / f"{artifact_prefix}.rendered.sql"
    manifest_path = pathlib.Path(args.output) if args.output else out_dir / f"{artifact_prefix}.prepare.json"

    resolve_cmd = [
        sys.executable,
        str(resolve_script),
        "--model",
        args.model,
        "--repo-root",
        str(repo_root),
        "--external-database",
        args.external_database,
        "--transformation-database",
        args.transformation_database,
        "--external-template",
        args.access_to_external_cluster,
        "--network",
        args.network,
        "--bounds-start",
        str(bounds_start),
        "--bounds-end",
        str(bounds_end),
        "--task-start",
        str(task_start),
        "--output",
        str(resolve_json),
    ]

    resolve_proc = run(resolve_cmd)
    if resolve_proc.returncode not in (0, 2):
        raise PrepError(
            "resolve_model_sql.py failed\n"
            f"stdout:\n{resolve_proc.stdout}\n"
            f"stderr:\n{resolve_proc.stderr}"
        )
    require_file(resolve_json, "resolve json")
    resolve_data = load_json(resolve_json)

    rendered = str(resolve_data.get("rendered_sql", "")).strip()
    rendered_sql.write_text(rendered + "\n", encoding="utf-8")

    introspect_cmd = [
        sys.executable,
        str(introspect_script),
        "--resolve-json",
        str(resolve_json),
        "--external-endpoint",
        args.external_endpoint,
        "--external-database",
        args.external_database,
        "--external-username",
        args.external_username,
        "--external-password",
        args.external_password,
        "--transformation-endpoint",
        args.transformation_endpoint,
        "--transformation-database",
        args.transformation_database,
        "--transformation-username",
        args.transformation_username,
        "--transformation-password",
        args.transformation_password,
        "--timeout",
        str(args.timeout),
        "--output",
        str(schema_json),
    ]

    introspect_proc = run(introspect_cmd)
    introspect_error = None
    schema_data = {}
    if introspect_proc.returncode == 0:
        require_file(schema_json, "schema json")
        schema_data = load_json(schema_json)
    else:
        introspect_error = (
            "introspect_tables.py failed\n"
            f"stdout:\n{introspect_proc.stdout}\n"
            f"stderr:\n{introspect_proc.stderr}"
        )

    unresolved = resolve_data.get("unresolved_fragments", [])
    versions = resolve_clickhouse_versions(args)
    version_probe_ok = all("error" not in info for info in versions.values())
    schema_summary = schema_data.get("summary", {}) if isinstance(schema_data, dict) else {}
    failed_tables = int(schema_summary.get("failed_tables", 0)) if schema_summary else 0
    manifest = {
        "paths": {
            "prepare_json": str(manifest_path),
            "resolve_json": str(resolve_json),
            "schemas_json": str(schema_json),
            "rendered_sql": str(rendered_sql),
        },
        "session_id": session_id,
        "artifact_prefix": artifact_prefix,
        "model_path": str(model_path),
        "clickhouse_versions": versions,
        "period": {
            **period_info,
            "bounds_start": bounds_start,
            "bounds_end": bounds_end,
            "task_start": task_start,
        },
        "resolve": resolve_data,
        "schema": schema_data,
        "unresolved_count": len(unresolved),
        "unresolved_fragments": unresolved,
        "summary": {
            "resolve": resolve_data.get("summary", {}),
            "schema": schema_summary,
            "version_probe_ok": version_probe_ok,
            "is_runnable": len(unresolved) == 0,
            "introspection_ok": introspect_proc.returncode == 0 and failed_tables == 0,
        },
    }
    if introspect_error:
        manifest["introspection_error"] = introspect_error

    output = json.dumps(manifest, indent=2)
    manifest_path.write_text(output + "\n", encoding="utf-8")
    print(output)

    if introspect_proc.returncode != 0:
        return introspect_proc.returncode
    if unresolved:
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PrepError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
