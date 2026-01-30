CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance_daily_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `date` Date COMMENT 'The date for this daily snapshot' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),

    -- Epoch range
    `start_epoch` UInt32 COMMENT 'First epoch of the day for this validator' CODEC(DoubleDelta, ZSTD(1)),
    `end_epoch` UInt32 COMMENT 'Last epoch of the day for this validator' CODEC(DoubleDelta, ZSTD(1)),

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
) PARTITION BY toStartOfMonth(date)
ORDER BY (date, validator_index)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily validator balance snapshots aggregated from per-epoch data';

CREATE TABLE `${NETWORK_NAME}`.fct_validator_balance_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_validator_balance_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_validator_balance_daily_local,
    cityHash64(date, validator_index)
);

ALTER TABLE `${NETWORK_NAME}`.fct_validator_balance_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_validator
(
    SELECT *
    ORDER BY (validator_index, date)
);
