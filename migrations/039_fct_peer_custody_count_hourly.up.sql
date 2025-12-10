CREATE TABLE `${NETWORK_NAME}`.fct_peer_custody_count_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `custody_group_count` UInt8 COMMENT 'Number of custody groups (1-128)' CODEC(ZSTD(1)),
    `epoch_count` UInt32 COMMENT 'Number of epochs in this hour' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Number of distinct peers with this custody group count in this hour' CODEC(ZSTD(1)),
    `min_epoch_peer_count` UInt32 COMMENT 'Minimum peer count across epochs in this hour' CODEC(ZSTD(1)),
    `max_epoch_peer_count` UInt32 COMMENT 'Maximum peer count across epochs in this hour' CODEC(ZSTD(1)),
    `avg_epoch_peer_count` Float64 COMMENT 'Average peer count across epochs in this hour' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (
    hour_start_date_time,
    custody_group_count
)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Peer custody count distribution aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_peer_custody_count_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_peer_custody_count_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_peer_custody_count_hourly_local,
    cityHash64(
        hour_start_date_time,
        custody_group_count
    )
);
