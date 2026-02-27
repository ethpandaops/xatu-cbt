# ClickHouse Optimization Playbook (High-Impact Only)

Use this checklist when reviewing benchmark results and query plans.

## Connection Rule
- Use ClickHouse HTTP endpoint on port `8123` only.
- Use `curl` style requests or script HTTP clients.
- Do not use `clickhouse-client` or native protocol/port `9000`.

## Benchmarking Discipline
- Disable query-level caches in benchmark runs:
  - `use_query_cache = 0`
  - `use_query_condition_cache = 0`
- Run warmup + repeated measured runs; report median and spread.
- Note caveat: filesystem/page caches can still affect wall-clock timings.
- Compare at least two time windows to avoid overfitting to one slice.
- For every candidate optimization, compare against baseline in each tested window (not just one window).
- Keep a result matrix with explicit negative cases; reject candidates that regress in important windows.
- Avoid ad-hoc shell timing math (for example `bc`-based elapsed calculations); prefer the provided Python benchmark wrappers for stable `wall_time_s` output.

## Plan and Read Analysis
- Use `EXPLAIN ESTIMATE` for read volume (`rows`, `marks`, `parts`).
- Use `EXPLAIN indexes = 1` to validate index and partition pruning.
- Check `system.query_log` for `read_rows`, `read_bytes`, and memory metrics.

## Schema-Aware Checks
- Collect `SHOW CREATE TABLE` for every dependency.
- For `Distributed` tables, also inspect the target table (usually `_local`).
- For `cluster(...)` queries, always reference `_local` table names rather than Distributed table names.
- Use `_local` engine/order/partition definitions for optimization reasoning.
- Use `_local` `ORDER BY` as the effective primary key if `PRIMARY KEY` is not explicitly set.
- For ReplacingMergeTree family, validate whether `FINAL` is truly necessary.
- Never assume `FINAL` is safe to remove just because current data shows no duplicates; duplicates can appear later as merges/versioned updates arrive.
- When versioning is driven by `update_date_time`, test an alternative dedup pattern using `argMax(column, update_date_time)` instead of `FINAL`.
- Recommend this rewrite only if result equivalence checks pass and benchmarks show meaningful wins in latency/read/memory.

## Join and Key Risk Rules
- Prefer join predicates aligned with ordering/primary keys where possible.
- Flag joins on sparse high-cardinality keys (address/username-like keys):
  - Can force broad scans and poor partition pruning.
  - Often look good in theory but inflate read and memory costs.
- Recommend readability-preserving structural changes only when impact is substantial.


## Correctness Gate
- For any optimized rewrite, compare baseline vs candidate and require exact match before accepting the rewrite.
- Version-gate `FORMAT Hash`:
  - ClickHouse `>= 25.8`: use `FORMAT Hash`.
  - ClickHouse `< 25.8`: skip `FORMAT Hash` and go straight to exact fallback.
- If `FORMAT Hash` is attempted and server returns `UNKNOWN_FORMAT`, fallback to:
  - `ORDER BY tuple(*)`
  - `FORMAT RowBinary`
  - SHA256 over the byte stream
- RowBinary fallback is value-based (not type-header-based), which avoids false mismatches from type-encoding differences.
- Never claim exact-result equivalence without a successful hash comparison result (`hashes_match = true`).


## Version-Aware Research
- Before optimization, get current ClickHouse versions from cluster metadata and use web search for version-specific guidance.
- Perform this research before major benchmark/rewrite cycles when feasible; if a quick baseline already ran, complete research before final recommendations.
- Prioritize official docs and release/changelog entries matching the running version, then scan newer versions for relevant performance fixes.
- Do not apply guidance blindly from mismatched versions without validating applicability.

## Recommendation Bar
Only recommend changes when evidence suggests meaningful gain, such as:
- Large read reduction (`read_rows`/`read_bytes`)
- Clear latency improvement (median and worst-case)
- Lower peak memory usage

Do not recommend noisy micro-optimizations that degrade readability.

## Reporting Contract
- Include a `Research Evidence` section with detected version(s), search queries used, and consulted source links.
- In `Hash Correctness Check`, report hash method used and note value-based semantics when `rowbinary_sha256_fallback` is used.
- When using `run_hash_check.sh`, treat `HASH_STATUS=mismatch` as candidate rejection even if shell exit is zero (default behavior). Use `HASH_MISMATCH_FAIL=1` for strict non-zero on mismatch.
- Include a comparison table with one row per `candidate x window`.
- Show baseline vs candidate metrics plus percent deltas for latency, read bytes, and peak memory.
- Include `hashes_match` in the table and never recommend rows where it is false.
- Include full final recommended query text (or explicitly state no rewrite is recommended).

## Sources
- https://clickhouse.com/docs/optimize/query-optimization
- https://clickhouse.com/docs/operations/query-cache
- https://clickhouse.com/docs/operations/settings/settings
- https://clickhouse.com/docs/engines/table-engines/special/distributed
- https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree
- https://clickhouse.com/docs/guides/joining-tables
- https://kb.altinity.com/altinity-kb-queries-and-syntax/joins/joins-tricks/
- https://kb.altinity.com/altinity-kb-queries-and-syntax/altinity-kb-final-clause-speed/
- https://kb.altinity.com/engines/mergetree-table-engine-family/replacingmergetree/
- https://kb.altinity.com/engines/mergetree-table-engine-family/pick-keys/
- https://kb.altinity.com/altinity-kb-queries-and-syntax/skip-indexes/
