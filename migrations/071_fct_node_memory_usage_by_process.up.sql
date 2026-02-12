-- Transformation output table for node memory usage with node_class enrichment
CREATE TABLE `${NETWORK_NAME}`.fct_node_memory_usage_by_process_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `window_start` DateTime64(3) COMMENT 'Start of the sub-slot aggregation window' CODEC(DoubleDelta, ZSTD(1)),
    `wallclock_slot` UInt32 COMMENT 'The wallclock slot number' CODEC(DoubleDelta, ZSTD(1)),
    `wallclock_slot_start_date_time` DateTime64(3) COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the observoor client that collected the data',
    `meta_network_name` LowCardinality(String) COMMENT 'Ethereum network name',
    `pid` UInt32 COMMENT 'Process ID of the monitored client' CODEC(ZSTD(1)),
    `client_type` LowCardinality(String) COMMENT 'Client type: CL or EL',
    `vm_rss_bytes` UInt64 COMMENT 'Resident set size in bytes (total physical memory used)' CODEC(ZSTD(1)),
    `rss_anon_bytes` UInt64 COMMENT 'Anonymous RSS in bytes (heap, stack, anonymous mmap)' CODEC(ZSTD(1)),
    `rss_file_bytes` UInt64 COMMENT 'File-backed RSS in bytes (shared libraries, mmap files)' CODEC(ZSTD(1)),
    `vm_swap_bytes` UInt64 COMMENT 'Swap usage in bytes' CODEC(ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification for filtering (e.g. eip7870)'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(wallclock_slot_start_date_time)
ORDER BY
    (wallclock_slot_start_date_time, meta_client_name, client_type, pid, window_start)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Node memory usage per sub-slot window enriched with node classification';

CREATE TABLE `${NETWORK_NAME}`.fct_node_memory_usage_by_process ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_node_memory_usage_by_process_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_node_memory_usage_by_process_local,
    cityHash64(wallclock_slot_start_date_time, meta_client_name)
);
