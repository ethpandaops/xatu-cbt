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
3. **Descriptive Context**: Intermediate models explicitly describe transformations and observers
4. **Plural Entities**: Use plural forms (e.g., `blocks`, `validators`, `transactions`)
5. **Observer Context**: Include data observer when relevant (`by_sentries`, `by_clients`, `by_peers`)

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

## Component Ordering for Complex Names

For intermediate models with multiple components, follow this order:
1. **Entity** (`blocks` or compound like `block_proposers`)
2. **Metric/Derivation** (`first_seen`, `latency`)
3. **Chain State** (`canonical` if needed)
4. **Observer** (`by_sentries`)
5. **Time Window** (`daily` if applicable)

Examples with simple entities:
- `int_blocks_first_seen_by_sentries`
- `int_blocks_first_seen_canonical_by_sentries`
- `int_blocks_first_seen_by_sentries_daily`
- `int_validators_performance_p50_by_region_hourly`

Examples with compound entities:
- `int_block_proposers_latency_by_sentries`
- `int_attestation_proposers_effectiveness_canonical_by_region`
- `int_sync_committees_participation_by_network_daily`

When combining observers: `by_all_nodes` or `by_network` instead of listing multiple.

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