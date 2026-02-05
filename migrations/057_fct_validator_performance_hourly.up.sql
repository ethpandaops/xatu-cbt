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
COMMENT 'Hourly aggregation of per-validator attestation vote correctness';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_vote_correctness_by_validator_hourly_local,
    cityHash64(validator_index, hour_start_date_time)
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
COMMENT 'Hourly validator balance snapshots aggregated from per-epoch data';

CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_validator_balance_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_validator_balance_hourly_local,
    cityHash64(validator_index, hour_start_date_time)
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
COMMENT 'Hourly aggregation of per-validator sync committee participation';

CREATE TABLE `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_sync_committee_participation_by_validator_hourly_local,
    cityHash64(validator_index, hour_start_date_time)
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
