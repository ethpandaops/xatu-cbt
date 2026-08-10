-- Daily aggregation of node CPU utilization
CREATE TABLE `${NETWORK_NAME}`.fct_node_cpu_utilization_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the observoor client that collected the data',
    `meta_network_name` LowCardinality(String) COMMENT 'Ethereum network name',
    `node_class` LowCardinality(String) COMMENT 'Node classification for filtering (e.g. eip7870)',
    `system_cores` UInt16 COMMENT 'Total system CPU cores' CODEC(ZSTD(1)),
    `hour_count` UInt32 COMMENT 'Number of source hourly slots in this day' CODEC(ZSTD(1)),
    `avg_core_pct` Float32 COMMENT 'Weighted average total CPU core utilization percentage' CODEC(ZSTD(1)),
    `min_core_pct` Float32 COMMENT 'Minimum total CPU core utilization percentage' CODEC(ZSTD(1)),
    `max_core_pct` Float32 COMMENT 'Maximum total CPU core utilization percentage' CODEC(ZSTD(1)),
    `p50_core_pct` Float32 COMMENT 'Weighted 50th percentile total CPU core utilization' CODEC(ZSTD(1)),
    `p95_core_pct` Float32 COMMENT 'Maximum of hourly 95th percentile CPU utilization' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (day_start_date, meta_client_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily aggregated node CPU utilization statistics per node';

CREATE TABLE `${NETWORK_NAME}`.fct_node_cpu_utilization_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_node_cpu_utilization_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_node_cpu_utilization_daily_local,
    cityHash64(day_start_date, meta_client_name)
);
