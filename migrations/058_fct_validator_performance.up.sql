-- ============================================================================
-- fct_attestation_vote_correctness_by_validator (per-slot)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Slot dimension
    `slot` UInt64 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time of the slot' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Per-slot metrics
    `attested` Bool COMMENT 'Whether the validator attested in this slot',
    `head_correct` Nullable(Bool) COMMENT 'Whether the head vote was correct. NULL if not attested' CODEC(ZSTD(1)),
    `target_correct` Nullable(Bool) COMMENT 'Whether the target vote was correct. NULL if not attested' CODEC(ZSTD(1)),
    `source_correct` Nullable(Bool) COMMENT 'Whether the source vote was correct. NULL if not attested' CODEC(ZSTD(1)),
    `inclusion_distance` Nullable(UInt32) COMMENT 'Inclusion distance for the attestation. NULL if not attested' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (validator_index, slot_start_date_time)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-slot attestation vote correctness by validator';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_vote_correctness_by_validator_local,
    cityHash64(validator_index, slot_start_date_time)
);

ALTER TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot_start_date_time
(
    SELECT *
    ORDER BY (slot_start_date_time, validator_index)
);

-- ============================================================================
-- fct_sync_committee_participation_by_validator (per-slot)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Slot dimension
    `slot` UInt64 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time of the slot' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'Index of the validator' CODEC(ZSTD(1)),

    -- Per-slot metrics
    `participated` Bool COMMENT 'Whether the validator participated in sync committee for this slot'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (validator_index, slot_start_date_time)
COMMENT 'Per-slot sync committee participation by validator';

CREATE TABLE `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_sync_committee_participation_by_validator_local,
    cityHash64(validator_index, slot_start_date_time)
);

-- ============================================================================
-- fct_validator_balance (per-epoch)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Epoch dimension
    `epoch` UInt32 COMMENT 'The epoch number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The start time of the epoch' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Per-epoch metrics (in Gwei)
    `balance` UInt64 COMMENT 'Validator balance at this epoch in Gwei' CODEC(T64, ZSTD(1)),
    `effective_balance` UInt64 COMMENT 'Effective balance at this epoch in Gwei' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Validator status at this epoch',
    `slashed` Bool COMMENT 'Whether the validator was slashed (as of this epoch)'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(epoch_start_date_time)
ORDER BY (validator_index, epoch_start_date_time)
COMMENT 'Per-epoch validator balance and status';

CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_validator_balance_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_validator_balance_local,
    cityHash64(validator_index, epoch_start_date_time)
);

-- ============================================================================
-- fct_attestation_vote_correctness_by_validator_hourly
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_hourly_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `hour_start_date_time` DateTime COMMENT 'The start of the hour for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Aggregated metrics per hour
    `total_duties` UInt32 COMMENT 'Total attestation duties for the validator in this hour' CODEC(ZSTD(1)),
    `attested_count` UInt32 COMMENT 'Number of attestations made' CODEC(ZSTD(1)),
    `missed_count` UInt32 COMMENT 'Number of attestations missed' CODEC(ZSTD(1)),
    `head_correct_count` UInt32 COMMENT 'Number of head votes that were correct' CODEC(ZSTD(1)),
    `target_correct_count` UInt32 COMMENT 'Number of target votes that were correct' CODEC(ZSTD(1)),
    `source_correct_count` UInt32 COMMENT 'Number of source votes that were correct' CODEC(ZSTD(1)),
    `avg_inclusion_distance` Nullable(Float32) COMMENT 'Average inclusion distance for attested slots. NULL if no attestations' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (validator_index, hour_start_date_time)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly aggregation of per-validator attestation vote correctness';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_vote_correctness_by_validator_hourly_local,
    cityHash64(validator_index, hour_start_date_time)
);

ALTER TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_hour_start_date_time
(
    SELECT *
    ORDER BY (hour_start_date_time, validator_index)
);

-- ============================================================================
-- fct_validator_balance_hourly
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance_hourly_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `hour_start_date_time` DateTime COMMENT 'The start of the hour for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Epoch range
    `start_epoch` UInt32 COMMENT 'First epoch in this hour for this validator' CODEC(DoubleDelta, ZSTD(1)),
    `end_epoch` UInt32 COMMENT 'Last epoch in this hour for this validator' CODEC(DoubleDelta, ZSTD(1)),

    -- Balance metrics (in Gwei)
    `start_balance` Nullable(UInt64) COMMENT 'Balance at start of hour (first epoch) in Gwei' CODEC(T64, ZSTD(1)),
    `end_balance` Nullable(UInt64) COMMENT 'Balance at end of hour (last epoch) in Gwei' CODEC(T64, ZSTD(1)),
    `min_balance` Nullable(UInt64) COMMENT 'Minimum balance during the hour in Gwei' CODEC(T64, ZSTD(1)),
    `max_balance` Nullable(UInt64) COMMENT 'Maximum balance during the hour in Gwei' CODEC(T64, ZSTD(1)),

    -- End of hour state
    `effective_balance` Nullable(UInt64) COMMENT 'Effective balance at end of hour in Gwei' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Validator status at end of hour',
    `slashed` Bool COMMENT 'Whether the validator was slashed (as of end of hour)'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (validator_index, hour_start_date_time)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly validator balance snapshots aggregated from per-epoch data';

CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_validator_balance_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_validator_balance_hourly_local,
    cityHash64(validator_index, hour_start_date_time)
);

ALTER TABLE `${NETWORK_NAME}`.fct_validator_balance_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_hour_start_date_time
(
    SELECT *
    ORDER BY (hour_start_date_time, validator_index)
);

-- ============================================================================
-- fct_sync_committee_participation_by_validator_hourly
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_hourly_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `hour_start_date_time` DateTime COMMENT 'The start of the hour for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'Index of the validator' CODEC(ZSTD(1)),

    -- Aggregated metrics per hour
    `total_slots` UInt32 COMMENT 'Total sync committee slots for the validator in this hour' CODEC(ZSTD(1)),
    `participated_count` UInt32 COMMENT 'Number of slots where validator participated' CODEC(ZSTD(1)),
    `missed_count` UInt32 COMMENT 'Number of slots where validator missed' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (validator_index, hour_start_date_time)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly aggregation of per-validator sync committee participation';

CREATE TABLE `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_sync_committee_participation_by_validator_hourly_local,
    cityHash64(validator_index, hour_start_date_time)
);

ALTER TABLE `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_hour_start_date_time
(
    SELECT *
    ORDER BY (hour_start_date_time, validator_index)
);

-- ============================================================================
-- fct_attestation_vote_correctness_by_validator_daily
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_daily_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `day_start_date` Date COMMENT 'The start of the day for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Aggregated metrics per day
    `total_duties` UInt32 COMMENT 'Total attestation duties for the validator in this day' CODEC(ZSTD(1)),
    `attested_count` UInt32 COMMENT 'Number of attestations made' CODEC(ZSTD(1)),
    `missed_count` UInt32 COMMENT 'Number of attestations missed' CODEC(ZSTD(1)),
    `head_correct_count` UInt32 COMMENT 'Number of head votes that were correct' CODEC(ZSTD(1)),
    `target_correct_count` UInt32 COMMENT 'Number of target votes that were correct' CODEC(ZSTD(1)),
    `source_correct_count` UInt32 COMMENT 'Number of source votes that were correct' CODEC(ZSTD(1)),
    `avg_inclusion_distance` Nullable(Float32) COMMENT 'Average inclusion distance for attested slots. NULL if no attestations' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (validator_index, day_start_date)
COMMENT 'Daily aggregation of per-validator attestation vote correctness';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_vote_correctness_by_validator_daily_local,
    cityHash64(validator_index, day_start_date)
);

-- ============================================================================
-- fct_validator_balance_daily
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance_daily_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `day_start_date` Date COMMENT 'The start of the day for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Epoch range
    `start_epoch` UInt32 COMMENT 'First epoch in this day for this validator' CODEC(DoubleDelta, ZSTD(1)),
    `end_epoch` UInt32 COMMENT 'Last epoch in this day for this validator' CODEC(DoubleDelta, ZSTD(1)),

    -- Balance metrics (in Gwei)
    `start_balance` Nullable(UInt64) COMMENT 'Balance at start of day (first epoch) in Gwei' CODEC(T64, ZSTD(1)),
    `end_balance` Nullable(UInt64) COMMENT 'Balance at end of day (last epoch) in Gwei' CODEC(T64, ZSTD(1)),
    `min_balance` Nullable(UInt64) COMMENT 'Minimum balance during the day in Gwei' CODEC(T64, ZSTD(1)),
    `max_balance` Nullable(UInt64) COMMENT 'Maximum balance during the day in Gwei' CODEC(T64, ZSTD(1)),

    -- End of day state
    `effective_balance` Nullable(UInt64) COMMENT 'Effective balance at end of day in Gwei' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Validator status at end of day',
    `slashed` Bool COMMENT 'Whether the validator was slashed (as of end of day)'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (validator_index, day_start_date)
COMMENT 'Daily validator balance snapshots aggregated from per-epoch data';

CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_validator_balance_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_validator_balance_daily_local,
    cityHash64(validator_index, day_start_date)
);

-- ============================================================================
-- fct_sync_committee_participation_by_validator_daily
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_daily_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `day_start_date` Date COMMENT 'The start of the day for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'Index of the validator' CODEC(ZSTD(1)),

    -- Aggregated metrics per day
    `total_slots` UInt32 COMMENT 'Total sync committee slots for the validator in this day' CODEC(ZSTD(1)),
    `participated_count` UInt32 COMMENT 'Number of slots where validator participated' CODEC(ZSTD(1)),
    `missed_count` UInt32 COMMENT 'Number of slots where validator missed' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (validator_index, day_start_date)
COMMENT 'Daily aggregation of per-validator sync committee participation';

CREATE TABLE `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_sync_committee_participation_by_validator_daily_local,
    cityHash64(validator_index, day_start_date)
);

-- ============================================================================
-- fct_block_proposer_by_validator (per-slot, re-indexed by validator)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_block_proposer_by_validator_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Slot dimension
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The validator index of the proposer' CODEC(ZSTD(1)),
    `pubkey` String COMMENT 'The public key of the proposer' CODEC(ZSTD(1)),

    -- Block
    `block_root` Nullable(String) COMMENT 'The beacon block root hash. NULL if missed' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Can be "canonical", "orphaned" or "missed"'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (validator_index, slot_start_date_time)
COMMENT 'Block proposers re-indexed by validator for efficient validator lookups';

CREATE TABLE `${NETWORK_NAME}`.fct_block_proposer_by_validator ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_block_proposer_by_validator_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_proposer_by_validator_local,
    cityHash64(validator_index, slot_start_date_time)
);

CREATE TABLE `${NETWORK_NAME}`.dim_validator_status_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `version` UInt32 COMMENT 'ReplacingMergeTree version: 4294967295 - epoch, keeps earliest epoch' CODEC(ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),
    `pubkey` String COMMENT 'The public key of the validator' CODEC(ZSTD(1)),

    -- Status
    `status` LowCardinality(String) COMMENT 'Beacon chain validator status (e.g. pending_initialized, active_ongoing)' CODEC(ZSTD(1)),

    -- First observed epoch for this status
    `epoch` UInt32 COMMENT 'First epoch this status was observed' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Wall clock time of the first observed epoch' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator lifecycle fields at the time of first observation
    `activation_epoch` Nullable(UInt64) COMMENT 'Epoch when activation is/was scheduled' CODEC(ZSTD(1)),
    `activation_eligibility_epoch` Nullable(UInt64) COMMENT 'Epoch when validator became eligible for activation' CODEC(ZSTD(1)),
    `exit_epoch` Nullable(UInt64) COMMENT 'Epoch when exit is/was scheduled' CODEC(ZSTD(1)),
    `withdrawable_epoch` Nullable(UInt64) COMMENT 'Epoch when withdrawal becomes possible' CODEC(ZSTD(1)),
    `slashed` Bool COMMENT 'Whether the validator was slashed at this transition'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `version`
) PARTITION BY toStartOfMonth(epoch_start_date_time)
ORDER BY (validator_index, status)
COMMENT 'Validator lifecycle status transitions — one row per (validator_index, status) with the first epoch observed';

CREATE TABLE `${NETWORK_NAME}`.dim_validator_status ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.dim_validator_status_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_validator_status_local,
    cityHash64(validator_index, status)
);

-- ============================================================================
-- dim_validator_pubkey (validator index <-> pubkey mapping)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.dim_validator_pubkey_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),
    `pubkey` String COMMENT 'The public key of the validator' CODEC(ZSTD(1)),

    PROJECTION p_by_validator_index
    (
        SELECT *
        ORDER BY (validator_index)
    )
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY (pubkey, validator_index)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Validator index to pubkey mapping — one row per validator with the latest pubkey';

CREATE TABLE `${NETWORK_NAME}`.dim_validator_pubkey ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.dim_validator_pubkey_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_validator_pubkey_local,
    cityHash64(pubkey, validator_index)
);
