# Xatu CBT

## Overview

Xatu CBT contains the models and tests for the [xatu](https://github.com/ethpandaops/xatu) project. [CBT](https://github.com/ethpandaops/cbt) is a lightweight clickhouse only tool, similar to DBT/sqlmesh, that allows you to define models and run them against a clickhouse database to perform transformations.

## Quirks

- Models are defined once but reused for multiple ethereum networks.
- We template `${NETWORK_NAME}` as the database in the [migrations](./migrations).
- Models must just reference tables in the same database, as we want to be able to reuse the models across many networks.
- Tests run against a spec, not a specific network.

## File Organisation

- `models/external` - contains the external models in a flat file structure where the file name is the table name.
- `models/transformations` - contains the transformations models in a flat file structure where the file name is the table name.
- `migrations` - database agnostic [go-migrate](https://github.com/golang-migrate/migrate) migrations that are run against the database.
- `tests/${spec}` - contains the tests for the models for a spec, typically an ethereum fork.

## Testing

The following structure is expected for each test spec:

```
tests/
├── ${spec}/
│   ├── data/                      # Parquet data configuration
│   │   └── canonical_beacon_block.yaml
│   └── assertions/                # SQL assertions
│       └── canonical_beacon_block.yaml
```

- Each test spec **MUST** have a file for every external model in the data directory.
- Each test spec **MUST** have a file for every model (external and transformations) in the assertions directory.

## Models

### External Models

- Defines an external data source for transformations and provides SQL on how to get the current bounds of the data available as min/max integers.
- While we create views for each `default.table` to `$network.table` in the [migrations](./migrations), for performance reasons we do not use that for the external model definitions.
- External models should be filtered by the primary partition column for incremental scanning.
- For DateTime columns, we should use the `toUnixTimestamp` function to convert to a Unix timestamp integer as CBT expects integers for min/max bounds.

#### Example

```sql
---
table: beacon_api_eth_v1_events_block
cache:
  incremental_scan_interval: 5s
  full_scan_interval: 24h
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
-- Use the default database as predicate pushdown does not work with views.
-- This gives 2-3x the performance.
-- Once we move the data into the mainnet database, we no longer need this.
FROM `default`.`{{ .self.table }}`
WHERE 
    meta_network_name = 'mainnet'
{{ if .cache.is_incremental_scan }}
    AND (
      slot_start_date_time <= fromUnixTimestamp({{ .cache.previous_min }})
      OR slot_start_date_time >= fromUnixTimestamp({{ .cache.previous_max }})
    )
{{ end }}
```

### Transformations Models

- Transformation models should use the `$network.external_table` view if using an external model as a dependency.

#### Naming Conventions

Model names follow dbt's stakeholder-friendly naming convention, adapted for Ethereum data:

**Structure:** `<layer>_<source>__<additional_context>`

##### Layer Prefixes (DAG stages)

- **`stg_`** - Staging: Raw data views/filters from xatu tables
- **`int_`** - Intermediate: Join/aggregation building blocks for composable transformations (includes time-based rollups)
- **`fct_`** - Facts: Event-based transactional data suitable for analysis
- **`dim_`** - Dimensions: Reference/lookup data for enrichment

##### Source Identifiers

The source describes the primary entity or domain. Can be single or compound:
- Single: `address`, `validator`, `block`, `token`
- Compound: `validator_performance`, `token_transfers`, `gas_prices`

Common sources:
- `address` / `account` - Address/account data
- `validator` / `validator_performance` - Validator metrics
- `block` / `block_producer` - Block data
- `token` / `token_transfers` - Token activity
- `gas` / `gas_prices` - Gas metrics
- `slot` / `slot_proposer` - Slot-level data
- `mev` / `mev_bundles` - MEV activity

##### Additional Context (after `__`)

Additional context describes **how** or **when**, not what:

**Chain State (critical for Ethereum):**
- `__canonical` - Only finalized/canonical chain data (won't reorg)
- `__head` - Including unfinalized head chain (may reorg)
- `__pending` - Mempool/pending transactions (not yet included)
- **Default assumption: If no state specified, assume canonical for facts/dims, head for staging/intermediate**

**Time Windows:**
- `__daily`, `__hourly`, `__weekly`, `__monthly`
- `__7d`, `__30d`, `__1h` (rolling windows)

**Occurrence:**
- `__first`, `__last`, `__unique`
- `__latest` (most recent snapshot)

##### Examples by Layer

**Staging Models (data filtering/preparation):**
```sql
stg_beacon_blocks                     -- Filtered beacon blocks
stg_token_transfers                   -- ERC20/721 transfer events  
stg_validator_duties                  -- Validator duty assignments
```

**Intermediate Models (reusable building blocks):**
```sql
int_address__first_access             -- First activity per address (canonical)
int_address__last_access              -- Last activity per address (canonical)
int_slot_proposer__canonical          -- Finalized block proposers only
int_slot_proposer__head               -- Including unfinalized proposers
int_validator_performance__daily      -- Daily performance (canonical)
int_gas_prices__hourly                -- Hourly gas price stats (head)
```

**Fact Tables (business events ready for analysis):**
```sql
fct_blocks                            -- Finalized blocks (default canonical)
fct_blocks__head                      -- Including unfinalized blocks
fct_attestations                      -- Finalized attestations only
fct_token_transfers                   -- Finalized transfers only
```

**Dimension Tables (reference/lookup data):**
```sql
dim_validators__latest                -- Current validator registry
dim_tokens                            -- Token metadata registry
dim_epochs                            -- Epoch calendar
```

##### Naming Rationale

1. **Clear data lineage** - The prefix immediately indicates the transformation stage
2. **Domain clarity** - Middle section identifies the data domain/source
3. **Entity specificity** - Clear indication of what entity is being modeled
4. **Transformation transparency** - Optional suffix clarifies any specific transformation applied
5. **Stakeholder-friendly** - Non-technical users can understand `fct_validator__attestations` represents attestation facts for validators
6. **Ethereum-native** - Uses terminology familiar to Ethereum developers and researchers
7. **Scalable** - Easy to extend for new domains (L2s, new protocols, cross-chain)

##### Best Practices

- Keep names descriptive but concise
- Use single underscore after layer prefix (e.g., `stg_beacon`)
- Use double underscore before additional context (e.g., `__daily`)
- Use singular forms for sources (e.g., `validator` not `validators`)
- Additional context is optional - only add when it clarifies the model's purpose
- Be consistent with source names across all layers

**Handling Finality:**
- For fact/dimension tables, default to canonical (finalized) data unless specified
- Use `__head` suffix when including unfinalized data for real-time analysis
- Use `__canonical` suffix when you need to be explicit about finalized-only data
- Consider creating both versions when users need both (e.g., `fct_blocks` and `fct_blocks__head`)
