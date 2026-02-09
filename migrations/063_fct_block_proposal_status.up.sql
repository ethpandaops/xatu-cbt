-- fct_block_proposal_status_hourly
CREATE TABLE `${NETWORK_NAME}`.fct_block_proposal_status_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Block proposal status (canonical, orphaned, missed)' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Number of slots with this status' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time, status)
COMMENT 'Hourly block proposal status counts by status type';

CREATE TABLE `${NETWORK_NAME}`.fct_block_proposal_status_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_block_proposal_status_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_proposal_status_hourly_local,
    cityHash64(hour_start_date_time)
);

-- fct_block_proposal_status_daily
CREATE TABLE `${NETWORK_NAME}`.fct_block_proposal_status_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Block proposal status (canonical, orphaned, missed)' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Number of slots with this status' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date, status)
COMMENT 'Daily block proposal status counts by status type';

CREATE TABLE `${NETWORK_NAME}`.fct_block_proposal_status_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_block_proposal_status_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_proposal_status_daily_local,
    cityHash64(day_start_date)
);
