-- Daily aggregation of node memory usage
CREATE TABLE `${NETWORK_NAME}`.fct_node_memory_usage_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the observoor client that collected the data',
    `meta_network_name` LowCardinality(String) COMMENT 'Ethereum network name',
    `node_class` LowCardinality(String) COMMENT 'Node classification for filtering (e.g. eip7870)',
    `hour_count` UInt32 COMMENT 'Number of source hourly slots in this day' CODEC(ZSTD(1)),
    `avg_vm_rss_bytes` UInt64 COMMENT 'Weighted average total RSS memory in bytes' CODEC(ZSTD(1)),
    `min_vm_rss_bytes` UInt64 COMMENT 'Minimum total RSS memory in bytes' CODEC(ZSTD(1)),
    `max_vm_rss_bytes` UInt64 COMMENT 'Maximum total RSS memory in bytes' CODEC(ZSTD(1)),
    `avg_rss_anon_bytes` UInt64 COMMENT 'Weighted average total anonymous RSS memory in bytes' CODEC(ZSTD(1)),
    `avg_rss_file_bytes` UInt64 COMMENT 'Weighted average total file-backed RSS memory in bytes' CODEC(ZSTD(1)),
    `avg_vm_swap_bytes` UInt64 COMMENT 'Weighted average total swap memory in bytes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (day_start_date, meta_client_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily aggregated node memory usage statistics per node';

CREATE TABLE `${NETWORK_NAME}`.fct_node_memory_usage_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_node_memory_usage_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_node_memory_usage_daily_local,
    cityHash64(day_start_date, meta_client_name)
);
