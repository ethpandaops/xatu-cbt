CREATE TABLE `${NETWORK_NAME}`.int_peer_head_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `event_date_time` DateTime COMMENT 'Timestamp when the status was observed' CODEC(DoubleDelta, ZSTD(1)),
    `peer_id_unique_key` String COMMENT 'Unique key for the peer' CODEC(ZSTD(1)),
    `head_slot` UInt32 COMMENT 'The head slot reported by the peer' CODEC(DoubleDelta, ZSTD(1)),
    `head_root` String COMMENT 'The head block root reported by the peer' CODEC(ZSTD(1)),
    `fork_digest` LowCardinality(String) COMMENT 'The fork digest reported by the peer' CODEC(ZSTD(1)),
    `finalized_epoch` Nullable(UInt32) COMMENT 'The finalized epoch reported by the peer' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Client implementation (e.g., lighthouse, prysm)' CODEC(ZSTD(1)),
    `client_version` LowCardinality(String) COMMENT 'Client version' CODEC(ZSTD(1)),
    `platform` LowCardinality(String) COMMENT 'Platform/OS' CODEC(ZSTD(1)),
    `country` LowCardinality(String) COMMENT 'Country name' CODEC(ZSTD(1)),
    `country_code` LowCardinality(String) COMMENT 'Country code' CODEC(ZSTD(1)),
    `continent_code` LowCardinality(String) COMMENT 'Continent code' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfDay(event_date_time)
ORDER BY (`head_slot`, `peer_id_unique_key`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-peer head observations from handle_status events, enriched with metadata';

CREATE TABLE `${NETWORK_NAME}`.int_peer_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_peer_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_peer_head_local,
    cityHash64(`head_slot`, `peer_id_unique_key`)
);
