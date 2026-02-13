-- Hourly aggregation of node network I/O across all processes
CREATE TABLE `${NETWORK_NAME}`.fct_node_network_io_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the observoor client that collected the data',
    `meta_network_name` LowCardinality(String) COMMENT 'Ethereum network name',
    `node_class` LowCardinality(String) COMMENT 'Node classification for filtering (e.g. eip7870)',
    `port_label` LowCardinality(String) COMMENT 'Port classification (e.g. cl_p2p_tcp, el_json_rpc)',
    `direction` LowCardinality(String) COMMENT 'Traffic direction: tx or rx',
    `slot_count` UInt32 COMMENT 'Number of slots in this hour' CODEC(ZSTD(1)),
    `sum_io_bytes` Float64 COMMENT 'Total bytes transferred in this hour' CODEC(ZSTD(1)),
    `avg_io_bytes` Float32 COMMENT 'Average bytes transferred per slot' CODEC(ZSTD(1)),
    `sum_io_count` UInt64 COMMENT 'Total packet count in this hour' CODEC(ZSTD(1)),
    `avg_io_count` UInt32 COMMENT 'Average packet count per slot' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time, meta_client_name, port_label, direction)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly aggregated node network I/O statistics per node, port, and direction';

CREATE TABLE `${NETWORK_NAME}`.fct_node_network_io_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_node_network_io_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_node_network_io_hourly_local,
    cityHash64(hour_start_date_time, meta_client_name)
);
