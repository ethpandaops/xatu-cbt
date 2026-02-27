---
name: optimize-model
description: Deeply optimize a Xatu CBT transformation model query using live ClickHouse evidence. Use when the user wants performance analysis (not code edits) for `models/transformations/*.sql`, including dependency rendering, schema introspection, benchmark runs, and high-impact recommendations.
compatibility: Requires Bash 3.2+, Python 3, jq, HTTP access to ClickHouse on port 8123, and internet access for web research.
argument-hint: "<model_path_or_name>"
---

# Optimize CBT Transformation Model (No Model Edits)

Analyze transformation model `$ARGUMENTS` and report high-impact ClickHouse performance improvements.

Never modify the model file. Only report findings and recommendations.

## Defaults To Confirm First

Before any analysis, collect and confirm (allow overrides):

- External models cluster:
  - `endpoint`: `http://chendpoint-xatu-clickhouse.analytics.production.ethpandaops:8123`
  - `default_database`: `default`
  - `username`: empty
  - `password`: empty
- Transformation models cluster:
  - `endpoint`: `http://chendpoint-xatu-cbt-clickhouse.analytics.production.ethpandaops:8123`
  - `default_database`: `mainnet`
  - `username`: empty
  - `password`: empty
  - `access_to_external_cluster`: `cluster('{remote_cluster}', database.table_name)`
  - Cluster substitution rule: when template uses `cluster(...)`, external dependencies are resolved to `<table>_local` automatically.

If any value is missing or ambiguous, ask follow-up questions before running benchmarks.

## Inputs

- Target transformation model (example): `models/transformations/fct_block.sql`
- Optional benchmark window overrides.
- Optional period mode:
  - `sane` (default): use frontmatter interval with protective caps to avoid timeouts.
  - `daring`: expand beyond sane range for deeper testing.

## Workflow

1. Resolve target model path
- Accept full path or model name.
- Normalize to `models/transformations/<name>.sql`.
- Stop with a clear error if not found.

2. Run one-shot preparation via wrapper (no flag discovery)
- Run:

```bash
SESSION_ID="$(date +%s)-$$"
PREP_OUTPUT="/tmp/optimize-model.${SESSION_ID}.prepare.json"

SESSION_ID="$SESSION_ID" PREP_OUTPUT="$PREP_OUTPUT" \
  .claude/skills/optimize-model/scripts/run_prepare.sh "$ARGUMENTS"
```

- This command generates session-isolated `/tmp` artifacts.
- Use `summary`, `period`, `unresolved_fragments`, and `summary.introspection_ok` from `$PREP_OUTPUT`.
- Confirm `period.bounds_start`/`period.bounds_end` are sane before benchmarking.
- If unresolved fragments remain, ask targeted follow-up questions and rerun.

3. Introspect dependency schemas
- This is already executed by prep wrapper.
- Always use `SHOW CREATE TABLE` for dependencies.
- If a dependency is `Distributed`, also inspect its target table (often `_local`) and reason from local engine/order/partition keys.
- Treat `_local` table `ORDER BY` as the effective primary key when `PRIMARY KEY` is not explicitly defined.
- For any `cluster(...)` table access, always target `_local` tables instead of Distributed table names.

4. Early version-aware research pass (recommended before benchmarking)
- Read ClickHouse versions from prep output first:
  - `jq '.clickhouse_versions' "$PREP_OUTPUT"`
- Use web search (explicitly) to review docs and changelog/release notes for the detected version before designing candidate rewrites.
- Prefer doing this before baseline/candidate benchmarking; if a quick baseline run already happened, continue and complete research before final recommendations.
- Use official ClickHouse docs first, then release/changelog pages relevant to the detected version and newer versions.
- Include version-aware search terms such as:
  - `clickhouse <major.minor> release notes`
  - `clickhouse <major.minor> query optimization`
  - `clickhouse replacingmergetree final performance`
- Use references:
  - `references/clickhouse-playbook.md`
  - `references/dependency-resolution.md`
  - `references/research-sources.md`

5. Benchmark rendered query (baseline, fast sample first)
- Run via wrapper:

```bash
SESSION_ID="$SESSION_ID" \
  .claude/skills/optimize-model/scripts/run_benchmark.sh "$PREP_OUTPUT"
```

- Wrapper output includes `BENCH_OUTPUT=...` path.
- Do not manually widen bounds unless user opts into `PERIOD_MODE=daring` or provides explicit bounds.
- The benchmark must disable query caches where supported.
- Note cache caveats (filesystem/page cache may still influence timings).
- Use explain output and query_log metrics for evidence.

6. Validate across multiple windows (required before recommendation)
- Use at least 2 windows:
  - `sane` window from frontmatter interval (default).
  - One additional window (`daring` or explicit custom bounds).
- Prefer 3 windows when runtime is acceptable (short/sane/daring) to reduce overfitting.
- For each window:
  - Produce window-specific baseline SQL via prep.
  - Benchmark baseline.
  - Benchmark each candidate rewrite derived from that same window-specific SQL via:

```bash
SESSION_ID="$SESSION_ID" WINDOW_LABEL="sane" CANDIDATE_LABEL="argmax" \
  .claude/skills/optimize-model/scripts/run_candidate_benchmark.sh "$PREP_OUTPUT" "/tmp/candidate.sql"
```

  - Run hash correctness checks baseline vs candidate for that same window.
- It is acceptable to keep candidates with negative impact in the comparison table; mark them as rejected.

7. Hash correctness gate (required for every rewritten query)
- For each candidate optimization SQL file, run:

```bash
SESSION_ID="$SESSION_ID" \
  .claude/skills/optimize-model/scripts/run_hash_check.sh "$PREP_OUTPUT" "/tmp/candidate.sql"
```

- Only treat a candidate as valid when `hashes_match = true`.
- If hash check fails, reject that candidate regardless of speed gains.
- Wrapper exit semantics:
  - Hash mismatches are reported via `HASH_STATUS=mismatch` and `HASH_MATCH=false` (non-fatal by default).
  - Set `HASH_MISMATCH_FAIL=1` to make mismatches return exit code `3`.
  - Execution/runtime failures still return non-zero and `HASH_STATUS=error`.
- Hash check script behavior:
  - Uses a version gate first:
    - ClickHouse `>= 25.8`: tries `FORMAT Hash`.
    - ClickHouse `< 25.8`: skips `FORMAT Hash` and directly uses exact byte-level comparison.
  - If `FORMAT Hash` is attempted but returns `UNKNOWN_FORMAT`, automatically falls back to exact byte-level comparison using:
    - `ORDER BY tuple(*)`
    - `FORMAT RowBinary` (value-based fallback)
    - SHA256 over the response stream
- If both methods fail, report that explicitly and do not claim exact-result equivalence.

8. Research-backed optimization pass
- Use findings from the early research pass to prioritize only high-impact candidates.
- Candidate rewrites are allowed in temporary SQL files (for benchmarking/hash checks), including moving/combining/removing CTEs when readability and performance both improve.

9. Recommendation filter
- Recommend only changes likely to produce meaningful gains.
- Do not trade readability for tiny gains.
- Explicitly evaluate memory impact.
- For ReplacingMergeTree tables, test whether `FINAL` can be removed and replaced with dedup logic such as `argMax(column, update_date_time)` when `update_date_time` is the version column.
- Do not treat “no duplicates right now” as sufficient evidence to remove `FINAL`; future duplicates are expected in ReplacingMergeTree workflows.
- Only recommend the `argMax(..., update_date_time)` pattern when benchmark evidence and result checks show correctness is preserved.
- Always flag sparse high-cardinality key join risks (address/username-like keys) when relevant.
- A candidate is recommended only if gains hold across multiple windows versus original baseline.

## Strict Rules

- Never edit model SQL files.
- Temporary candidate SQL rewrites in `/tmp` are allowed for experiments and benchmarking.
- Never propose materialized views or projections for this workflow.
- Always use HTTP on port `8123` for ClickHouse access (`curl` style / HTTP API).
- Never use `clickhouse-client` or any native-port (`9000`) workflow in this skill.
- Wrapper shell scripts must remain Bash `3.2+` compatible (macOS default bash).
- Do not run `--help` for skill scripts during normal flow; use the canonical commands documented above.
- If a script fails, inspect stderr and rerun with corrected arguments instead of probing help output.
- Always run hash correctness checks before recommending query rewrites.
- Keep recommendations evidence-based (benchmark + explain + schema context).
- If evidence is weak or mixed, explicitly say so.
- Final report must include a `Research Evidence` section with version(s), web search queries, and source links used.
- Final report must include a comparison table for baseline vs each candidate across tested windows.
- Final report must include the final recommended query text (or explicitly state no rewrite is recommended).

## Output Format

Provide exactly these sections:

1. `Input Confirmation`
2. `Rendered Runnable SQL`
3. `Dependency & Schema Findings`
4. `Research Evidence`
5. `Benchmark Results (with cache caveats)`
6. `Hash Correctness Check`
7. `Optimization Comparison Table`
8. `Top Optimizations (high impact only)`
9. `Recommended Final Query`
10. `What Not To Change`
11. `Open Questions`

`Research Evidence` requirements:
- Include detected ClickHouse version(s) used for analysis.
- Include the exact web search queries used.
- Include links to docs/changelog pages actually consulted.
- Include a short applicability note per key source (why it matters for this version/workload).

`Hash Correctness Check` requirements:
- Include which method was used: `format_hash` or `rowbinary_sha256_fallback`.
- For `rowbinary_sha256_fallback`, report that comparison semantics are value-based (`FORMAT RowBinary`).

`Optimization Comparison Table` requirements:
- One row per candidate per window.
- Include baseline and candidate metrics side by side and percentage deltas.
- Minimum columns:
  - `candidate`
  - `window_label`
  - `hashes_match`
  - `baseline_wall_time_s_median`
  - `candidate_wall_time_s_median`
  - `delta_wall_time_pct`
  - `baseline_read_bytes_median`
  - `candidate_read_bytes_median`
  - `delta_read_bytes_pct`
  - `baseline_peak_memory_usage_max`
  - `candidate_peak_memory_usage_max`
  - `delta_peak_memory_pct`
  - `decision` (`recommended` or `rejected`)

## Script Notes

- Wrapper prep: `.claude/skills/optimize-model/scripts/run_prepare.sh`
- Wrapper benchmark: `.claude/skills/optimize-model/scripts/run_benchmark.sh`
- Wrapper candidate benchmark: `.claude/skills/optimize-model/scripts/run_candidate_benchmark.sh`
- Wrapper hash check: `.claude/skills/optimize-model/scripts/run_hash_check.sh`
- Underlying prep: `.claude/skills/optimize-model/scripts/prepare_model_analysis.py`
- Resolver: `.claude/skills/optimize-model/scripts/resolve_model_sql.py`
- Schema introspection: `.claude/skills/optimize-model/scripts/introspect_tables.py`
- Benchmarking: `.claude/skills/optimize-model/scripts/benchmark_query.py`
- Hash compare: `.claude/skills/optimize-model/scripts/compare_query_hash.py`

If a script fails, surface exact error text and continue manually only if safety and correctness are preserved.
