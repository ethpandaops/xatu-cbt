CREATE TABLE `${NETWORK_NAME}`.fct_peer_custody_count_by_epoch_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt64 COMMENT 'Epoch number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'Start date time of the epoch' CODEC(DoubleDelta, ZSTD(1)),
    `custody_group_count` UInt8 COMMENT 'Number of custody groups (1-128)' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Number of distinct peers with this custody group count in this epoch' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY toYYYYMM(epoch_start_date_time)
ORDER BY (
    epoch,
    custody_group_count
)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Peer count by custody group count per epoch';

CREATE TABLE `${NETWORK_NAME}`.fct_peer_custody_count_by_epoch ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_peer_custody_count_by_epoch_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_peer_custody_count_by_epoch_local,
    cityHash64(
        epoch,
        custody_group_count
    )
);
