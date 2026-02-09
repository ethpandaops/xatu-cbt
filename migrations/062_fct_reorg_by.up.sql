-- fct_reorg_by_hourly
CREATE TABLE `${NETWORK_NAME}`.fct_reorg_by_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `depth` UInt16 COMMENT 'Reorg depth (number of consecutive orphaned slots)' CODEC(ZSTD(1)),
    `reorg_count` UInt32 COMMENT 'Number of reorg events at this depth' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time, depth)
COMMENT 'Hourly reorg event counts by depth';

CREATE TABLE `${NETWORK_NAME}`.fct_reorg_by_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_reorg_by_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_reorg_by_hourly_local,
    cityHash64(hour_start_date_time)
);

-- fct_reorg_by_daily
CREATE TABLE `${NETWORK_NAME}`.fct_reorg_by_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `depth` UInt16 COMMENT 'Reorg depth (number of consecutive orphaned slots)' CODEC(ZSTD(1)),
    `reorg_count` UInt32 COMMENT 'Number of reorg events at this depth' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date, depth)
COMMENT 'Daily reorg event counts by depth';

CREATE TABLE `${NETWORK_NAME}`.fct_reorg_by_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_reorg_by_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_reorg_by_daily_local,
    cityHash64(day_start_date)
);
