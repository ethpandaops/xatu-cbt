CREATE TABLE `${NETWORK_NAME}`.fct_block_first_seen_by_node_by_epoch_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the client',
    `meta_client_implementation` LowCardinality(String) COMMENT 'Implementation of the client',
    `meta_client_geo_city` LowCardinality(String) COMMENT 'City of the client' CODEC(ZSTD(1)),
    `meta_client_geo_country` LowCardinality(String) COMMENT 'Country of the client' CODEC(ZSTD(1)),
    `meta_client_geo_country_code` LowCardinality(String) COMMENT 'Country code of the client' CODEC(ZSTD(1)),
    `meta_client_geo_continent_code` LowCardinality(String) COMMENT 'Continent code of the client' CODEC(ZSTD(1)),
    `meta_client_geo_longitude` Nullable(Float64) COMMENT 'Longitude of the client' CODEC(ZSTD(1)),
    `meta_client_geo_latitude` Nullable(Float64) COMMENT 'Latitude of the client' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_number` Nullable(UInt32) COMMENT 'Autonomous system number of the client' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_organization` Nullable(String) COMMENT 'Autonomous system organization of the client' CODEC(ZSTD(1)),
    `min_slot` UInt64 COMMENT 'Minimum slot number in this epoch' CODEC(ZSTD(1)),
    `max_slot` UInt64 COMMENT 'Maximum slot number in this epoch' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Number of slots with blocks seen in this epoch' CODEC(ZSTD(1)),
    `min_seen_slot_start_diff_ms` UInt32 COMMENT 'Minimum time from slot start for the node to see a block (milliseconds)' CODEC(ZSTD(1)),
    `p05_seen_slot_start_diff_ms` UInt32 COMMENT '5th percentile time from slot start (milliseconds)' CODEC(ZSTD(1)),
    `p50_seen_slot_start_diff_ms` UInt32 COMMENT 'Median (p50) time from slot start (milliseconds)' CODEC(ZSTD(1)),
    `avg_seen_slot_start_diff_ms` UInt32 COMMENT 'Average time from slot start (milliseconds)' CODEC(ZSTD(1)),
    `p90_seen_slot_start_diff_ms` UInt32 COMMENT '90th percentile time from slot start (milliseconds)' CODEC(ZSTD(1)),
    `p95_seen_slot_start_diff_ms` UInt32 COMMENT '95th percentile time from slot start (milliseconds)' CODEC(ZSTD(1)),
    `p99_seen_slot_start_diff_ms` UInt32 COMMENT '99th percentile time from slot start (milliseconds)' CODEC(ZSTD(1)),
    `max_seen_slot_start_diff_ms` UInt32 COMMENT 'Maximum time from slot start (milliseconds)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(epoch_start_date_time)
ORDER BY
    (`epoch_start_date_time`, `username`, `node_id`, `classification`, `meta_client_name`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Block first seen statistics by node aggregated by epoch';

CREATE TABLE `${NETWORK_NAME}`.fct_block_first_seen_by_node_by_epoch ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_block_first_seen_by_node_by_epoch_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_first_seen_by_node_by_epoch_local,
    cityHash64(`epoch_start_date_time`, `username`, `node_id`, `classification`, `meta_client_name`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_block_first_seen_by_node_by_epoch_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_epoch_node
(
    SELECT *
    ORDER BY (`epoch`, `username`, `node_id`, `classification`, `meta_client_name`)
);
