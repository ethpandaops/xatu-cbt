# Research Sources For Query Optimization

Use official ClickHouse docs first, then use Altinity KB/blog content for practical production tuning patterns and troubleshooting.

## Official ClickHouse Sources

- Query optimization workflow:
  - https://clickhouse.com/docs/optimize/query-optimization
- PREWHERE optimization:
  - https://clickhouse.com/docs/optimize/prewhere
- Choosing primary key / ORDER BY:
  - https://clickhouse.com/docs/best-practices/choosing-a-primary-key
- Data skipping indexes:
  - https://clickhouse.com/docs/optimize/skipping-indexes
- JOIN optimization:
  - https://clickhouse.com/docs/guides/joining-tables
- EXPLAIN reference:
  - https://clickhouse.com/docs/sql-reference/statements/explain
- ReplacingMergeTree behavior:
  - https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree
- Query cache:
  - https://clickhouse.com/docs/operations/query-cache
- Query condition cache:
  - https://clickhouse.com/docs/operations/query-condition-cache
- Query parallelism:
  - https://clickhouse.com/docs/optimize/query-parallelism

## Official ClickHouse Release/Engineering References

- Release 25.10 (runtime join filters and other optimizer improvements):
  - https://clickhouse.com/blog/clickhouse-release-25-10
- Release 25.12 (join reordering enhancements):
  - https://clickhouse.com/blog/clickhouse-release-25-12
- Engineering optimization guide:
  - https://clickhouse.com/resources/engineering/clickhouse-query-optimisation-definitive-guide
- Lazy materialization deep dive:
  - https://clickhouse.com/blog/clickhouse-gets-lazier-and-faster-introducing-lazy-materialization
- Top-N granule-level skipping:
  - https://clickhouse.com/blog/clickhouse-top-n-queries-granule-level-data-skipping

## Altinity References (Practical Tuning + Troubleshooting)

- JOIN tricks and query patterns:
  - https://kb.altinity.com/altinity-kb-queries-and-syntax/joins/joins-tricks/
- FINAL clause speed notes:
  - https://kb.altinity.com/altinity-kb-queries-and-syntax/altinity-kb-final-clause-speed/
- ReplacingMergeTree specifics:
  - https://kb.altinity.com/engines/mergetree-table-engine-family/replacingmergetree/
- Pick keys (ORDER BY / PRIMARY KEY schema design):
  - https://kb.altinity.com/engines/mergetree-table-engine-family/pick-keys/
- Skip indexes:
  - https://kb.altinity.com/altinity-kb-queries-and-syntax/skip-indexes/
- Query profiling with trace log:
  - https://kb.altinity.com/altinity-kb-queries-and-syntax/trace_log/
- Memory troubleshooting and controls:
  - https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-memory-overcommit/
- Aggregation memory pressure background:
  - https://kb.altinity.com/altinity-kb-queries-and-syntax/group-by/tricks/
- Slow `count()` troubleshooting:
  - https://kb.altinity.com/altinity-kb-queries-and-syntax/slow_select_count/
- Row-level dedup tradeoffs:
  - https://kb.altinity.com/altinity-kb-schema-design/row-level-deduplication/

## Usage Guidance

- Prefer official docs for semantics and current feature behavior.
- Use release notes/blogs to align recommendations with running ClickHouse version.
- Use Altinity KB to generate practical experiments, then validate with benchmark + hash checks across multiple windows.
