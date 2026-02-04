---
name: optimize-model
description: Analyze and optimize a CBT transformation model by testing against production data
argument-hint: "<model_name>"
---

# Optimize CBT Transformation Model

Analyze the transformation model `$ARGUMENTS` and suggest optimizations.

## Prerequisites

**CRITICAL**: Before doing anything else, verify the ethpandaops MCP is available.

1. Use `ListMcpResourcesTool` to check for `ethpandaops-mcp` server
2. If NOT available, STOP and show this error:

```
ERROR: ethpandaops MCP server is not available.

This skill requires the ethpandaops MCP to query production ClickHouse data.

To install and configure the MCP server:
  https://github.com/ethpandaops/mcp

After installation, restart Claude Code and try again.
```

1. If available, run a quick connectivity test using `execute_python`:

```python
from ethpandaops import clickhouse
# Simple connectivity check
result = clickhouse.query("xatu", "SELECT 1")
print("MCP connection OK")
```

If the connection test fails, show the same error message above.

## Workflow

1. **Read the model file** from `models/transformations/$ARGUMENTS.sql`

2. **Parse the model** to extract (see [CBT Templating](#cbt-templating) for reference):
   - Dependencies (external vs transformation)
   - Query structure (CTEs, JOINs, aggregations)
   - Partition filters

3. **Get schema information** for dependencies:

   **For external model dependencies** - query ClickHouse `xatu` cluster:

   ```python
   from ethpandaops import clickhouse
   # Get full table definition including ENGINE, ORDER BY, PARTITION BY
   schema = clickhouse.query("xatu", "SHOW CREATE TABLE {table_name}")
   print(schema.iloc[0, 0])  # Full CREATE TABLE statement
   ```

   This reveals:
   - Engine type (ReplacingMergeTree means FINAL may be needed)
   - ORDER BY keys (critical for query optimization)
   - PARTITION BY (for partition pruning)

   **For transformation model dependencies** - read from `migrations/` folder:
   - Search for the migration file that creates the table: `grep -l "CREATE TABLE.*{table_name}" migrations/*.up.sql`
   - Read the `.up.sql` migration file to get the full schema
   - Example: `migrations/011_block.up.sql` contains schema for `fct_block`

4. **Test against production** using ethpandaops MCP:
   - External dependencies -> query `xatu` cluster with `FROM table WHERE meta_network_name = 'mainnet'`
   - Transformation dependencies -> query `xatu-cbt` cluster with `FROM mainnet.table`
   - Use a recent 1-hour time window for testing
   - Measure query execution time
   - Note: ClickHouse caches results - use `SETTINGS use_query_cache = 0` to bypass, or run multiple times for average/median
   - Use `EXPLAIN ESTIMATE` to get rows/marks/parts to be read without executing:

     ```sql
     EXPLAIN ESTIMATE SELECT ... FROM table WHERE ...
     ```

   - Use `EXPLAIN indexes = 1` to check index usage and partition pruning

5. **Research optimization techniques**:
   - Web search for latest ClickHouse optimization best practices
   - Check ClickHouse docs for relevant features
   - NEVER recommend materialized views
   - Avoid PREWHERE - usually over-optimization

6. **Verify correctness**: When testing before/after optimizations, MUST check multiple chunks of data and ensure results keep the same hash:

   ```sql
   SELECT ... FORMAT Hash
   ```

7. **Analyze and suggest optimizations** such as:
   - FINAL usage optimization (avoid if not needed)
   - Join strategy (GLOBAL vs local joins)
   - Partition pruning improvements
   - Column selection reduction
   - Pre-filtering opportunities
   - CTE vs subquery trade-offs

## Anti-Patterns

**Partition explosion on JOINs**: Be wary of JOINs on tables partitioned by non-incrementing, high-cardinality fields (like Ethereum addresses `0xabc...`).

**Over-optimization**: Don't sacrifice readability for tiny performance gains. If a query is acceptable, leave it alone.

## MCP Usage

Use `execute_python` with the ethpandaops library:

```python
from ethpandaops import clickhouse
import time

# For external tables (raw data)
result = clickhouse.query("xatu", """
    SELECT ... FROM table_name
    WHERE meta_network_name = 'mainnet'
    AND slot_start_date_time BETWEEN '...' AND '...'
""")

# For transformation tables (pre-aggregated)
result = clickhouse.query("xatu-cbt", """
    SELECT ... FROM mainnet.table_name
    WHERE slot_start_date_time BETWEEN '...' AND '...'
""")
```

## CBT Templating

Use `mainnet` as the network name when testing.

ClickHouse is clustered, so JOINs most likely need the `GLOBAL` keyword.

Cross-cluster access via the `cluster()` function:

```sql
-- From xatu cluster, access xatu-cbt tables
SELECT * FROM cluster('{cbt_cluster}', mainnet.fct_block)

-- From xatu-cbt cluster, access xatu tables
SELECT * FROM cluster('{remote_cluster}', default.canonical_beacon_block)
```

## Output Format

Provide a summary with:

- Model overview (dependencies, structure)
- Test results (row counts, timing)
- Optimization suggestions with rationale
- Recommended changes (if any)
