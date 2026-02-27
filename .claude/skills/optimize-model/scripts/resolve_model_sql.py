#!/usr/bin/env python3
"""Resolve a CBT transformation model into a runnable read-only SQL query.

The script:
- Normalizes a transformation model path.
- Extracts dependencies from frontmatter and dep helper calls.
- Classifies dependencies as external/transformation using repo files.
- Replaces common dep index helpers, including:
  - {{ index .dep "{{external|transformation}}" "table" "helpers" "from" }}
  - {{ index .dep "{{external|transformation}}" "table" "database" }}
- Substitutes common CBT template variables used in transformation models.
- Optionally strips INSERT INTO to keep benchmarking read-only.
- Reports unresolved template fragments so the caller can ask follow-up questions.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import re
import sys
from typing import Dict, List, Tuple

DEP_INDEX_CALL_RE = re.compile(
    r"\{\{\s*index\s+\.dep\s+\"(\{\{(?:external|transformation)\}\})\"\s+\"([A-Za-z0-9_]+)\"(?:\s+\"([A-Za-z0-9_]+)\"(?:\s+\"([A-Za-z0-9_]+)\")?)?\s*\}\}"
)
FRONTMATTER_DEP_RE = re.compile(
    r"^\s*-\s*[\"']?\{\{(external|transformation)\}\}\.([A-Za-z0-9_]+)[\"']?\s*$"
)
UNRESOLVED_TEMPLATE_RE = re.compile(r"\{\{[-]?\s*[^{}]+?\s*[-]?\}\}")
INSERT_PREFIX_RE = re.compile(
    r"^\s*INSERT\s+INTO\b.*?(?=\bWITH\b|\bSELECT\b)",
    flags=re.IGNORECASE | re.DOTALL,
)


class ResolveError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, help="Model name or path")
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--external-database", default="default")
    parser.add_argument("--transformation-database", default="mainnet")
    parser.add_argument(
        "--external-template",
        default="cluster('{remote_cluster}', database.table_name)",
        help="Template used for external dependencies",
    )
    parser.add_argument("--network", default="mainnet")
    parser.add_argument("--bounds-start", type=int, default=now - 3600)
    parser.add_argument("--bounds-end", type=int, default=now)
    parser.add_argument("--task-start", type=int, default=now)
    parser.add_argument(
        "--keep-insert",
        action="store_true",
        help="Keep INSERT INTO prefix (default is read-only query)",
    )
    parser.add_argument("--output", help="Write JSON output to file")
    return parser.parse_args()


def find_repo_root(start: pathlib.Path) -> pathlib.Path:
    current = start.resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "models" / "transformations").is_dir() and (
            candidate / "models" / "external"
        ).is_dir():
            return candidate
    raise ResolveError(
        "Could not find repo root containing models/transformations and models/external"
    )


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

    # Handle name.sql passed without folder.
    if raw.suffix == ".sql":
        candidates.append(repo_root / "models" / "transformations" / raw.name)

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    hint = repo_root / "models" / "transformations"
    raise ResolveError(f"Model not found for '{model_arg}'. Looked under {hint}")


def split_frontmatter(content: str) -> Tuple[str, str]:
    if not content.startswith("---\n"):
        return "", content
    marker = "\n---\n"
    idx = content.find(marker, 4)
    if idx == -1:
        return "", content
    frontmatter = content[4:idx]
    body = content[idx + len(marker) :]
    return frontmatter, body


def parse_frontmatter_dependencies(frontmatter: str) -> List[Tuple[str, str]]:
    deps: List[Tuple[str, str]] = []
    in_dep_block = False
    for line in frontmatter.splitlines():
        if re.match(r"^\s*dependencies\s*:\s*$", line):
            in_dep_block = True
            continue
        if in_dep_block and re.match(r"^\S", line):
            in_dep_block = False
        if not in_dep_block:
            continue
        match = FRONTMATTER_DEP_RE.match(line)
        if match:
            deps.append((match.group(2), match.group(1)))
    return deps


def parse_frontmatter_interval(frontmatter: str) -> Dict[str, object]:
    interval: Dict[str, object] = {"type": None, "max": None}
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


def parse_dep_template_calls(sql_body: str) -> List[Tuple[str, str]]:
    parsed: List[Tuple[str, str]] = []
    for match in DEP_INDEX_CALL_RE.finditer(sql_body):
        dep_kind = "external" if "external" in match.group(1) else "transformation"
        table = match.group(2)
        parsed.append((table, dep_kind))
    return parsed


def infer_dep_kind(repo_root: pathlib.Path, table: str) -> str:
    ext = (repo_root / "models" / "external" / f"{table}.sql").is_file()
    trn = (repo_root / "models" / "transformations" / f"{table}.sql").is_file()
    if ext and not trn:
        return "external"
    if trn and not ext:
        return "transformation"
    return "unknown"


def render_external_ref(template: str, database: str, table: str) -> str:
    uses_cluster_function = re.search(r"\bcluster\s*\(", template, flags=re.IGNORECASE) is not None
    external_table = table
    if uses_cluster_function and not table.endswith("_local"):
        external_table = f"{table}_local"

    rendered = template
    rendered = rendered.replace("{database}", database)
    rendered = rendered.replace("{db}", database)
    rendered = rendered.replace("{table}", external_table)
    rendered = rendered.replace("{table_name}", external_table)
    rendered = re.sub(r"\bdatabase\.table_name\b", f"{database}.{external_table}", rendered)
    rendered = re.sub(r"\bdatabase\.table\b", f"{database}.{external_table}", rendered)
    return rendered


def replace_dep_helpers(
    sql_body: str,
    dep_kinds: Dict[str, str],
    external_database: str,
    transformation_database: str,
    external_template: str,
) -> str:
    def replacer(match: re.Match[str]) -> str:
        expected_kind = "external" if "external" in match.group(1) else "transformation"
        table = match.group(2)
        key1 = match.group(3)
        key2 = match.group(4)
        actual_kind = dep_kinds.get(table, expected_kind)

        # Keep the explicit kind from SQL call if classifier cannot resolve.
        chosen_kind = actual_kind if actual_kind != "unknown" else expected_kind

        # helpers.from => full table reference.
        if key1 == "helpers" and key2 == "from":
            if chosen_kind == "external":
                return render_external_ref(external_template, external_database, table)
            return f"`{transformation_database}`.`{table}`"

        # .database => database name only.
        if key1 == "database":
            if chosen_kind == "external":
                return external_database
            return transformation_database

        # .table => table name only.
        if key1 == "table":
            if chosen_kind == "external":
                uses_cluster_function = (
                    re.search(r"\bcluster\s*\(", external_template, flags=re.IGNORECASE)
                    is not None
                )
                if uses_cluster_function and not table.endswith("_local"):
                    return f"{table}_local"
            return table

        # Unknown accessor: leave template unresolved for explicit follow-up.
        return match.group(0)

    return DEP_INDEX_CALL_RE.sub(replacer, sql_body)


def replace_common_templates(
    sql_body: str,
    network: str,
    bounds_start: int,
    bounds_end: int,
    task_start: int,
) -> str:
    replacements = {
        r"\{\{\s*\.env\.NETWORK\s*\}\}": network,
        r"\{\{\s*\.bounds\.start\s*\}\}": str(bounds_start),
        r"\{\{\s*\.bounds\.end\s*\}\}": str(bounds_end),
        r"\{\{\s*\.task\.start\s*\}\}": str(task_start),
        r"\{\{\s*\.self\.database\s*\}\}": "benchmark_db",
        r"\{\{\s*\.self\.table\s*\}\}": "benchmark_table",
    }
    out = sql_body
    for pattern, value in replacements.items():
        out = re.sub(pattern, value, out)
    return out


def maybe_strip_insert(sql_body: str, keep_insert: bool) -> str:
    if keep_insert:
        return sql_body
    if not re.match(r"^\s*INSERT\s+INTO\b", sql_body, flags=re.IGNORECASE):
        return sql_body
    stripped = INSERT_PREFIX_RE.sub("", sql_body, count=1)
    return stripped.lstrip()


def collect_unresolved(sql_body: str) -> List[str]:
    unresolved = sorted(set(UNRESOLVED_TEMPLATE_RE.findall(sql_body)))
    return unresolved


def main() -> int:
    args = parse_args()
    repo_root = find_repo_root(pathlib.Path(args.repo_root))
    model_path = normalize_model_path(repo_root, args.model)

    content = model_path.read_text(encoding="utf-8")
    frontmatter, body = split_frontmatter(content)
    interval = parse_frontmatter_interval(frontmatter)

    fm_deps = parse_frontmatter_dependencies(frontmatter)
    tpl_deps = parse_dep_template_calls(body)

    dep_map: Dict[str, str] = {}
    dep_sources: Dict[str, List[str]] = {}

    def merge_dep(table: str, kind: str, source: str) -> None:
        prev = dep_map.get(table)
        if prev is None:
            dep_map[table] = kind
        elif prev != kind and "unknown" not in (prev, kind):
            dep_map[table] = "unknown"
        elif prev == "unknown" and kind != "unknown":
            dep_map[table] = kind
        dep_sources.setdefault(table, []).append(source)

    for table, kind in fm_deps:
        merge_dep(table, kind, "frontmatter")
    for table, kind in tpl_deps:
        merge_dep(table, kind, "dep_helper")

    for table in list(dep_map.keys()):
        inferred = infer_dep_kind(repo_root, table)
        if dep_map[table] == "unknown" and inferred != "unknown":
            dep_map[table] = inferred
        elif dep_map[table] != inferred and inferred != "unknown":
            # Keep explicit model value, but report ambiguity.
            dep_sources[table].append(f"inferred:{inferred}")

    rendered = replace_dep_helpers(
        body,
        dep_map,
        external_database=args.external_database,
        transformation_database=args.transformation_database,
        external_template=args.external_template,
    )
    rendered = replace_common_templates(
        rendered,
        network=args.network,
        bounds_start=args.bounds_start,
        bounds_end=args.bounds_end,
        task_start=args.task_start,
    )
    rendered = maybe_strip_insert(rendered, keep_insert=args.keep_insert)

    unresolved = collect_unresolved(rendered)

    deps = [
        {
            "table": table,
            "kind": dep_map[table],
            "sources": dep_sources.get(table, []),
        }
        for table in sorted(dep_map)
    ]
    dependency_counts: Dict[str, int] = {"external": 0, "transformation": 0, "unknown": 0}
    for dep in deps:
        kind = str(dep.get("kind", "unknown"))
        dependency_counts[kind] = dependency_counts.get(kind, 0) + 1

    output = {
        "repo_root": str(repo_root),
        "model_path": str(model_path),
        "dependencies": deps,
        "rendered_sql": rendered.strip(),
        "unresolved_count": len(unresolved),
        "unresolved_fragments": unresolved,
        "unresolved_templates": unresolved,
        "summary": {
            "model": model_path.name,
            "interval": interval,
            "dependency_count": len(deps),
            "dependency_counts": dependency_counts,
            "unresolved_count": len(unresolved),
            "is_runnable": len(unresolved) == 0,
        },
    }

    payload = json.dumps(output, indent=2)
    if args.output:
        pathlib.Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)

    # Non-zero exit allows the caller to prompt for missing values.
    if unresolved:
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ResolveError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
