-- Weekly per-validator rollups built from the *_daily tables, for long time ranges
-- (e.g. "All") where shipping one row per day per validator is too much.
-- week_start_date is the Monday (UTC) that starts the ISO week.
-- Partitioned by year: a week fits in one partition and a validator's full history
-- touches one partition per year.

-- ============================================================================
-- fct_attestation_vote_correctness_by_validator_weekly
-- ============================================================================
CREATE TABLE fct_attestation_vote_correctness_by_validator_weekly_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `week_start_date` Date COMMENT 'The start of the week (Monday, UTC) for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Aggregated metrics per week
    `total_duties` UInt32 COMMENT 'Total attestation duties for the validator in this week' CODEC(ZSTD(1)),
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
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY (validator_index, week_start_date)
COMMENT 'Weekly aggregation of per-validator attestation vote correctness';

CREATE TABLE fct_attestation_vote_correctness_by_validator_weekly ON CLUSTER '{cluster}'
AS fct_attestation_vote_correctness_by_validator_weekly_local
ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_attestation_vote_correctness_by_validator_weekly_local,
    cityHash64(validator_index, week_start_date)
);

-- ============================================================================
-- fct_sync_committee_participation_by_validator_weekly
-- ============================================================================
CREATE TABLE fct_sync_committee_participation_by_validator_weekly_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `week_start_date` Date COMMENT 'The start of the week (Monday, UTC) for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'Index of the validator' CODEC(ZSTD(1)),

    -- Aggregated metrics per week
    `total_slots` UInt32 COMMENT 'Total sync committee slots for the validator in this week' CODEC(ZSTD(1)),
    `participated_count` UInt32 COMMENT 'Number of slots where validator participated' CODEC(ZSTD(1)),
    `missed_count` UInt32 COMMENT 'Number of slots where validator missed' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY (validator_index, week_start_date)
COMMENT 'Weekly aggregation of per-validator sync committee participation';

CREATE TABLE fct_sync_committee_participation_by_validator_weekly ON CLUSTER '{cluster}'
AS fct_sync_committee_participation_by_validator_weekly_local
ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_sync_committee_participation_by_validator_weekly_local,
    cityHash64(validator_index, week_start_date)
);

-- ============================================================================
-- fct_validator_balance_weekly
-- ============================================================================
CREATE TABLE fct_validator_balance_weekly_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `week_start_date` Date COMMENT 'The start of the week (Monday, UTC) for this aggregation' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Epoch range
    `start_epoch` UInt32 COMMENT 'First epoch in this week for this validator' CODEC(DoubleDelta, ZSTD(1)),
    `end_epoch` UInt32 COMMENT 'Last epoch in this week for this validator' CODEC(DoubleDelta, ZSTD(1)),

    -- Balance metrics (in Gwei)
    `start_balance` Nullable(UInt64) COMMENT 'Balance at start of week (first epoch) in Gwei' CODEC(T64, ZSTD(1)),
    `end_balance` Nullable(UInt64) COMMENT 'Balance at end of week (last epoch) in Gwei' CODEC(T64, ZSTD(1)),
    `min_balance` Nullable(UInt64) COMMENT 'Minimum balance during the week in Gwei' CODEC(T64, ZSTD(1)),
    `max_balance` Nullable(UInt64) COMMENT 'Maximum balance during the week in Gwei' CODEC(T64, ZSTD(1)),

    -- End of week state
    `effective_balance` Nullable(UInt64) COMMENT 'Effective balance at end of week in Gwei' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Validator status at end of week',
    `slashed` Bool COMMENT 'Whether the validator was slashed (as of end of week)'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(week_start_date)
ORDER BY (validator_index, week_start_date)
COMMENT 'Weekly validator balance snapshots aggregated from daily data';

CREATE TABLE fct_validator_balance_weekly ON CLUSTER '{cluster}'
AS fct_validator_balance_weekly_local
ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_validator_balance_weekly_local,
    cityHash64(validator_index, week_start_date)
);
