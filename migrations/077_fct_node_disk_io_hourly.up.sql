-- Hourly aggregation of node disk I/O across all processes
CREATE TABLE `${NETWORK_NAME}`.fct_node_disk_io_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the observoor client that collected the data',
    `meta_network_name` LowCardinality(String) COMMENT 'Ethereum network name',
    `node_class` LowCardinality(String) COMMENT 'Node classification for filtering (e.g. eip7870)',
    `rw` LowCardinality(String) COMMENT 'Read or write operation',
    `slot_count` UInt32 COMMENT 'Number of slots in this hour' CODEC(ZSTD(1)),
    `sum_io_bytes` Float64 COMMENT 'Total bytes transferred in this hour' CODEC(ZSTD(1)),
    `avg_io_bytes` Float32 COMMENT 'Average bytes transferred per slot' CODEC(ZSTD(1)),
    `sum_io_ops` UInt64 COMMENT 'Total I/O operations in this hour' CODEC(ZSTD(1)),
    `avg_io_ops` UInt32 COMMENT 'Average I/O operations per slot' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time, meta_client_name, rw)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly aggregated node disk I/O statistics per node and read/write direction';

CREATE TABLE `${NETWORK_NAME}`.fct_node_disk_io_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_node_disk_io_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_node_disk_io_hourly_local,
    cityHash64(hour_start_date_time, meta_client_name)
);
