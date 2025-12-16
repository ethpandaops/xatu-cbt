-- Hourly storage slot state (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`)
COMMENT 'Storage slot state metrics aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_hourly_local,
    cityHash64(`hour_start_date_time`)
);

-- Daily storage slot state (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (`day_start_date`)
COMMENT 'Storage slot state metrics aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_daily_local,
    cityHash64(`day_start_date`)
);

-- Hourly storage slot state with expiry (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour (with 6m expiry policy)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour (with 6m expiry policy)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`)
COMMENT 'Storage slot state metrics with 6-month expiry policy aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_6m_hourly_local,
    cityHash64(`hour_start_date_time`)
);

-- Daily storage slot state with expiry (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day (with 6m expiry policy)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day (with 6m expiry policy)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (`day_start_date`)
COMMENT 'Storage slot state metrics with 6-month expiry policy aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_6m_daily_local,
    cityHash64(`day_start_date`)
);

