# Naming Conventions

Model names follow dbt-aligned patterns optimized for Xatu's observability focus.

## Key Principles

1. **Double Underscore (`__`)**: Reserved ONLY for source/entity separation in staging/base layers
2. **Single Underscore (`_`)**: Used everywhere else for word separation
3. **Singular Entities**: Use singular forms (e.g., `block`, `validator`, `transaction`)
4. **Compound Entities**: Treat role-based entities as single units (e.g., `block_proposer`, not `block_proposers`)
5. **Aggregation Dimensions**: `by_` prefix indicates grouping (e.g., `by_node` = one row per node)
6. **Time Distinctions**:
   - `_daily`, `_hourly` = Aggregated time series (historical)
   - `_last_24h`, `_last_7d` = Rolling windows (current snapshot)
7. **Component Order**: Entity → Metric → State → Aggregation → Time

## Layer Prefixes (DAG stages)

- **`stg_`** - Staging: Raw data views/filters from xatu tables
- **`base_`** - Base: Cleaned and standardized source data
- **`int_`** - Intermediate: Join/aggregation building blocks for composable transformations
- **`fct_`** - Facts: Event-based transactional data suitable for analysis
- **`dim_`** - Dimensions: Reference/lookup data for enrichment

## Shared Concepts

These concepts apply across all transformation layers (int, fct):

### Entity Vocabulary

#### Simple Entities

- `block` - Beacon/execution block
- `transaction` - Execution layer transaction
- `attestation` - Validator attestation
- `validator` - Validator entity
- `client` - Consensus/execution client
- `node` - Sentry node observing the network
- `peer` - P2P network peer
- `slot` - Beacon chain slot
- `epoch` - Beacon chain epoch

#### Compound Entities

Compound entities represent specific roles or relationships and are treated as single units:

- `block_proposer` - Validator proposing block
- `attestation_proposer` - Validator proposing attestation
- `sync_committee` - Validator in sync committee
- `withdrawal_validator` - Validator with withdrawal

### Transformation Descriptors

Common descriptors for intermediate and fact models:

- `first_seen` - Initial observation of an entity
- `last_seen` - Most recent observation
- `first_access` - First access to an entity
- `last_access` - Most recent access to an entity
- `propagation` - Network propagation metrics
- `orphaned` - Blocks not in canonical chain
- `reorged` - Chain reorganization events
- `missed` - Missed slots/attestations
- `delayed` - Late arrivals
- `performance` - Performance metrics
- `effectiveness` - Effectiveness measurements
- `participation` - Participation rates

### Aggregation Dimensions

The `by_` prefix indicates data is grouped/aggregated by that dimension:

- `by_node` - One row per node
- `by_region` - One row per region  
- `by_client` - One row per client type
- `by_operator` - One row per operator

Note: `by_node` means "grouped by node", not necessarily "observed by node".

When aggregating across all dimensions: use `by_network` or `total` instead of listing multiple.

### Time Windows

#### Aggregation Granularity

Data aggregated into time buckets (historical time series):

- `_hourly` - One row per hour
- `_daily` - One row per day  
- `_weekly` - One row per week
- `_monthly` - One row per month

#### Rolling Windows

Current snapshot of recent data (refreshed continuously):

- `_last_1h` - Active/events in last hour
- `_last_24h` - Active/events in last 24 hours
- `_last_7d` - Active/events in last 7 days
- `_last_30d` - Active/events in last 30 days

### Component Ordering

For all transformation models (int, fct) with multiple components, follow this order:

1. **Entity** (`block` or compound like `block_proposer`)
2. **Metric/Purpose** (`first_seen`, `propagation`, `performance`)
3. **Chain State** (`canonical` if needed)
4. **Aggregation Dimension** (`by_node`, `by_region`, `by_client`)
5. **Time Granularity** (`daily`, `hourly`) or **Rolling Period** (`last_24h`, `last_7d`)

## Naming Patterns by Layer

**Pattern Notation:**

- `<component>` = Required component
- `[component]` = Optional component

### Staging & Base Models

```
Pattern: stg_<source>__<entity>
         base_<source>__<entity>

Examples:
stg_xatu__execution_block_observation
stg_xatu__beacon_api_event
base_xatu__canonical_execution_block
base_xatu__canonical_beacon_block
stg_xatu__mempool_transaction
```

### Intermediate Models

```
Pattern: int_<entity>_<derivation/metric>_[aggregation]_[time]

Note: <entity> can be a compound entity like block_proposer

Examples:
int_block_first_seen_by_node
int_block_first_seen_by_client
int_transaction_mempool_propagation
int_attestation_first_seen_by_peer
int_attestation_proposer_missed_by_region

With time:
int_block_propagation_by_node_daily
int_validator_performance_p50_by_region_hourly
int_node_active_last_24h
```

### Fact Models

```
Pattern: fct_<entity>_<metric/purpose>_[aggregation]_[time]

Note: Facts can be granular (event-level) or pre-aggregated

Examples (granular/event-level):
fct_block_propagation
fct_attestation_submission
fct_validator_proposal

Examples (with aggregation):
fct_block_propagation_by_region
fct_validator_performance_by_operator
fct_attestation_effectiveness_by_client

Examples (with time):
fct_block_propagation_daily
fct_validator_performance_hourly
fct_network_health_last_24h

Examples (fully specified):
fct_block_propagation_by_region_daily
fct_validator_attestation_performance_by_operator_hourly
fct_sync_committee_participation_by_client_last_7d
```

### Dimension Models

```
Pattern: dim_<entity>

Examples:
dim_validator
dim_client
dim_node
dim_peer
dim_region
```

## Handling Chain State

For models where finality matters:

- Include `canonical` or `head` in model names when distinction matters
- Example: `fct_attestation_correctness_head` vs `fct_attestation_correctness_canonical`

## Best Practices

- Keep names descriptive but concise
- Prioritize clarity over brevity
- Use consistent entity names across all layers
- Include aggregation context in intermediate and fact models
- Avoid abbreviations (use `transaction` not `txn`)

## Summary Table

| Layer          | Pattern                                        | Examples                                          |
|----------------|------------------------------------------------|---------------------------------------------------|
| **stg**        | `stg_<source>__<entity>`                       | `stg_xatu__beacon_api_event`                      |
| **base**       | `base_<source>__<entity>`                      | `base_xatu__canonical_execution_block`            |
| **int**        | `int_<entity>_<metric>_[aggregation]_[time]`   | `int_block_first_seen_by_node`                    |
|                |                                                | `int_block_propagation_by_node_daily`             |
| **fct**        | `fct_<entity>_<metric>_[aggregation]_[time]`   | `fct_block_propagation`                           |
|                |                                                | `fct_validator_performance_by_operator_hourly`    |
| **dim**        | `dim_<entity>`                                 | `dim_validator`, `dim_node`                       |
