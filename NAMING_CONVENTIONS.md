# Naming Conventions

Model names follow dbt-aligned patterns optimized for Xatu's observability focus.

## Layer Prefixes (DAG stages)

- **`stg_`** - Staging: Raw data views/filters from xatu tables
- **`base_`** - Base: Cleaned and standardized source data
- **`int_`** - Intermediate: Join/aggregation building blocks for composable transformations
- **`fct_`** - Facts: Event-based transactional data suitable for analysis
- **`dim_`** - Dimensions: Reference/lookup data for enrichment

## Naming Patterns by Layer

### Staging & Base Models

```
Pattern: stg_<source>__<entity>
         base_<source>__<entity>

Examples:
stg_xatu__execution_block_observations
stg_xatu__beacon_api_events
base_xatu__canonical_execution_blocks
base_xatu__canonical_beacon_blocks
stg_xatu__mempool_transactions
```

### Intermediate Models

```
Pattern: int_<entity>_<derivation/metric>_<context>

Note: <entity> can be a compound entity like block_proposers

Examples:
int_blocks_first_seen_by_sentries
int_blocks_first_seen_by_clients
int_blocks_propagation_latency
int_transactions_mempool_propagation
int_attestations_first_seen_by_peers
int_validators_proposals_with_latency
int_block_proposers_latency_by_sentries
int_attestation_proposers_missed_by_region
```

### Fact Models

```
Pattern: fct_<entity>_<event/metric>

Examples:
fct_block_propagation
fct_transaction_propagation
fct_attestation_observations
fct_validator_performance
```

### Dimension Models

```
Pattern: dim_<entity>

Examples:
dim_validators
dim_clients
dim_sentries
dim_peers
dim_regions
```

## Key Principles

1. **Double Underscore (`__`)**: Reserved ONLY for source/entity separation in staging/base layers
2. **Single Underscore (`_`)**: Used everywhere else for word separation
3. **Plural Entities**: Use plural forms (e.g., `blocks`, `validators`, `transactions`)
4. **Compound Entities**: Treat role-based entities as single units (e.g., `block_proposers`, not `blocks_proposers`)
5. **Aggregation Dimensions**: `by_` prefix indicates grouping (e.g., `by_sentries` = one row per sentry)
6. **Time Distinctions**:
   - `_daily`, `_hourly` = Aggregated time series (historical)
   - `_last_24h`, `_last_7d` = Rolling windows (current snapshot)
7. **Component Order**: Entity → Metric → State → Aggregation → Time

## Entity Vocabulary

Common entities in Xatu context:

### Simple Entities

- `blocks` - Beacon/execution blocks
- `transactions` - Execution layer transactions
- `attestations` - Validator attestations
- `validators` - Validator entities
- `clients` - Consensus/execution clients
- `sentries` - Sentry nodes observing the network
- `peers` - P2P network peers
- `slots` - Beacon chain slots
- `epochs` - Beacon chain epochs

### Compound Entities

Compound entities represent specific roles or relationships and are treated as single units:

- `block_proposers` - Validators proposing blocks
- `attestation_proposers` - Validators proposing attestations
- `sync_committees` - Validators in sync committees
- `withdrawal_validators` - Validators with withdrawals

Examples:

- `int_block_proposers_latency_by_sentries` - Latency of block proposers
- `int_attestation_proposers_effectiveness_by_region` - Effectiveness of attestation proposers
- `int_sync_committees_participation_daily` - Daily sync committee participation

## Transformation Descriptors

Common descriptors for intermediate models:

- `first_seen` - Initial observation of an entity
- `last_seen` - Most recent observation
- `propagation` - Network propagation metrics
- `latency` - Time-based measurements
- `orphaned` - Blocks not in canonical chain
- `reorged` - Chain reorganization events
- `missed` - Missed slots/attestations
- `delayed` - Late arrivals

## Aggregation Dimensions

The `by_` prefix indicates data is grouped/aggregated by that dimension:

- `by_sentries` - One row per sentry
- `by_region` - One row per region  
- `by_client` - One row per client type
- `by_operator` - One row per operator

Note: `by_sentries` means "grouped by sentry", not necessarily "observed by sentries".

## Time Windows

### Aggregation Granularity

Data aggregated into time buckets (historical time series):

- `_hourly` - One row per hour
- `_daily` - One row per day  
- `_weekly` - One row per week
- `_monthly` - One row per month

### Rolling Windows

Current snapshot of recent data (refreshed continuously):

- `_last_1h` - Active/events in last hour
- `_last_24h` - Active/events in last 24 hours
- `_last_7d` - Active/events in last 7 days
- `_last_30d` - Active/events in last 30 days

Examples:

- `int_validators_performance_daily` - Performance aggregated by day (time series)
- `int_validators_performance_last_24h` - Current performance snapshot (rolling)
- `int_nodes_active_last_24h` - Nodes currently active (rolling window)
- `int_blocks_propagation_hourly` - Propagation stats per hour (time series)

## Component Ordering for Complex Names

For intermediate models with multiple components, follow this order:

1. **Entity** (`blocks` or compound like `block_proposers`)
2. **Metric/Derivation** (`first_seen`, `latency`)
3. **Chain State** (`canonical` if needed)
4. **Aggregation Dimension** (`by_sentries`, `by_region`, `by_client`)
5. **Time Granularity** (`daily`, `hourly`) or **Rolling Period** (`last_24h`, `last_7d`)

Examples showing aggregation dimensions:

- `int_blocks_first_seen_by_sentries` (grouped by sentry)
- `int_blocks_first_seen_canonical_by_sentries`
- `int_blocks_propagation_by_sentries_daily` (one row per sentry per day)
- `int_validators_performance_p50_by_region_hourly` (one row per region per hour)

Examples with compound entities:

- `int_block_proposers_latency_by_sentries` (grouped by sentry)
- `int_attestation_proposers_effectiveness_canonical_by_region`
- `int_sync_committees_participation_by_client_daily` (one row per client per day)

Examples with rolling windows:

- `int_nodes_active_last_24h` (all active nodes, single snapshot)
- `int_validators_missed_attestations_by_operator_last_7d` (grouped by operator, rolling)
- `int_blocks_orphaned_last_1h` (all orphaned blocks in last hour)

When aggregating across all dimensions: use `by_network` or `total` instead of listing multiple.

## Handling Chain State

For models where finality matters:

- Default to canonical (finalized) data for facts/dimensions
- Include `canonical` or `head` in intermediate model names when distinction matters
- Example: `int_blocks_canonical_by_sentries` vs `int_blocks_head_by_sentries`

## Best Practices

- Keep names descriptive but concise
- Prioritize clarity over brevity
- Use consistent entity names across all layers
- Include observer context in intermediate models
- Avoid abbreviations (use `transactions` not `txns`)

## Summary Table

| Layer          | Pattern                                | Xatu Examples                              |
|----------------|----------------------------------------|--------------------------------------------|
| **stg**        | `stg_<source>__<entity>`               | `stg_xatu__beacon_api_events`             |
| **base**       | `base_<source>__<entity>`              | `base_xatu__canonical_execution_blocks`   |
| **int**        | `int_<entity>_<derivation>_<context>`  | `int_blocks_first_seen_by_sentries`       |
|                |                                        | `int_transactions_mempool_latency`        |
| **fct**        | `fct_<entity>_<event/metric>`          | `fct_block_propagation`                   |
|                |                                        | `fct_validator_performance`               |
| **dim**        | `dim_<entity>`                         | `dim_validators`, `dim_sentries`          |
