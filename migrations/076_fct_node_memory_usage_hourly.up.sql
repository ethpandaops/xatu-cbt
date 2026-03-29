-- Hourly aggregation of node memory usage across all processes
CREATE TABLE `${NETWORK_NAME}`.fct_node_memory_usage_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the observoor client that collected the data',
    `meta_network_name` LowCardinality(String) COMMENT 'Ethereum network name',
    `node_class` LowCardinality(String) COMMENT 'Node classification for filtering (e.g. eip7870)',
    `slot_count` UInt32 COMMENT 'Number of slots in this hour' CODEC(ZSTD(1)),
    `avg_vm_rss_bytes` UInt64 COMMENT 'Average total RSS memory in bytes' CODEC(ZSTD(1)),
    `min_vm_rss_bytes` UInt64 COMMENT 'Minimum total RSS memory in bytes' CODEC(ZSTD(1)),
    `max_vm_rss_bytes` UInt64 COMMENT 'Maximum total RSS memory in bytes' CODEC(ZSTD(1)),
    `avg_rss_anon_bytes` UInt64 COMMENT 'Average total anonymous RSS memory in bytes' CODEC(ZSTD(1)),
    `avg_rss_file_bytes` UInt64 COMMENT 'Average total file-backed RSS memory in bytes' CODEC(ZSTD(1)),
    `avg_vm_swap_bytes` UInt64 COMMENT 'Average total swap memory in bytes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time, meta_client_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly aggregated node memory usage statistics per node';

CREATE TABLE `${NETWORK_NAME}`.fct_node_memory_usage_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_node_memory_usage_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_node_memory_usage_hourly_local,
    cityHash64(hour_start_date_time, meta_client_name)
);
