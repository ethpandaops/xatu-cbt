-- TODO: Add columns for 1-year expiry policy once fct_storage_slot_state_with_expiry_by_1y is available:
--   `active_slots_with_1y_expiry` Int64 COMMENT 'Cumulative count of active storage slots at end of day (with 1-year expiry applied)' CODEC(ZSTD(1)),
--   `effective_bytes_with_1y_expiry` Int64 COMMENT 'Cumulative sum of effective bytes at end of day (with 1-year expiry applied)' CODEC(ZSTD(1))
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes across all active slots at end of day' CODEC(ZSTD(1)),
    `active_slots_with_six_months_expiry` Int64 COMMENT 'Cumulative count of active storage slots at end of day (with 6-month expiry applied)' CODEC(ZSTD(1)),
    `effective_bytes_with_six_months_expiry` Int64 COMMENT 'Cumulative sum of effective bytes at end of day (with 6-month expiry applied)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (`day_start_date`)
COMMENT 'Daily aggregation of storage slot state - tracks active slots and effective bytes with and without 6-month expiry policy';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_daily_local,
    cityHash64(`day_start_date`)
);
