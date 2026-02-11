-- fct_missed_slot_rate_hourly
CREATE TABLE `${NETWORK_NAME}`.fct_missed_slot_rate_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Total number of slots in this hour' CODEC(ZSTD(1)),
    `missed_count` UInt32 COMMENT 'Number of missed slots in this hour' CODEC(ZSTD(1)),
    `missed_rate` Float32 COMMENT 'Missed slot rate (%)' CODEC(ZSTD(1)),
    `moving_avg_missed_rate` Float32 COMMENT 'Moving average missed rate (6-hour window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time)
COMMENT 'Hourly missed slot rate with moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_missed_slot_rate_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_missed_slot_rate_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_missed_slot_rate_hourly_local,
    cityHash64(hour_start_date_time)
);

-- fct_missed_slot_rate_daily
CREATE TABLE `${NETWORK_NAME}`.fct_missed_slot_rate_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Total number of slots in this day' CODEC(ZSTD(1)),
    `missed_count` UInt32 COMMENT 'Number of missed slots in this day' CODEC(ZSTD(1)),
    `missed_rate` Float32 COMMENT 'Missed slot rate (%)' CODEC(ZSTD(1)),
    `moving_avg_missed_rate` Float32 COMMENT 'Moving average missed rate (7-day window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Daily missed slot rate with moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_missed_slot_rate_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_missed_slot_rate_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_missed_slot_rate_daily_local,
    cityHash64(day_start_date)
);
