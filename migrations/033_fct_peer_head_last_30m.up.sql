CREATE TABLE `${NETWORK_NAME}`.fct_peer_head_last_30m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `head_slot` UInt32 COMMENT 'The head slot reported by peers' CODEC(DoubleDelta, ZSTD(1)),
    `head_root` String COMMENT 'The head block root reported by peers (unknown if not available)' CODEC(ZSTD(1)),
    `peer_count` UInt32 COMMENT 'Total number of peers at this head' CODEC(ZSTD(1)),
    `count_by_client` Map(String, UInt32) COMMENT 'Peer count breakdown by client implementation' CODEC(ZSTD(1)),
    `count_by_country` Map(String, UInt32) COMMENT 'Peer count breakdown by country' CODEC(ZSTD(1)),
    `count_by_continent` Map(String, UInt32) COMMENT 'Peer count breakdown by continent code' CODEC(ZSTD(1)),
    `count_by_fork_digest` Map(String, UInt32) COMMENT 'Peer count breakdown by fork digest' CODEC(ZSTD(1)),
    `count_by_platform` Map(String, UInt32) COMMENT 'Peer count breakdown by platform (os)' CODEC(ZSTD(1)),
    `count_by_finalized_epoch` Map(String, UInt32) COMMENT 'Peer count breakdown by finalized epoch' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(updated_date_time)
ORDER BY (`head_slot`, `head_root`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated peer head distribution from the last 30 minutes of handle_status events';

CREATE TABLE `${NETWORK_NAME}`.fct_peer_head_last_30m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_peer_head_last_30m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_peer_head_last_30m_local,
    cityHash64(`head_slot`, `head_root`)
);
