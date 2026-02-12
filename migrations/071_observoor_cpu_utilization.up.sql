-- External model table mirroring observoor.cpu_utilization from production
CREATE TABLE `${NETWORK_NAME}`.observoor_cpu_utilization_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `window_start` DateTime64(3) COMMENT 'Start of the sub-slot aggregation window' CODEC(DoubleDelta, ZSTD(1)),
    `wallclock_slot` UInt32 COMMENT 'The wallclock slot number' CODEC(DoubleDelta, ZSTD(1)),
    `wallclock_slot_start_date_time` DateTime64(3) COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the observoor client that collected the data',
    `meta_network_name` LowCardinality(String) COMMENT 'Ethereum network name',
    `pid` UInt32 COMMENT 'Process ID of the monitored client' CODEC(ZSTD(1)),
    `client_type` LowCardinality(String) COMMENT 'Client type: consensus or execution client name',
    `system_cores` UInt16 COMMENT 'Total system CPU cores' CODEC(ZSTD(1)),
    `mean_core_pct` Float32 COMMENT 'Mean CPU core utilization percentage (100pct = 1 core)' CODEC(ZSTD(1)),
    `min_core_pct` Float32 COMMENT 'Minimum CPU core utilization percentage (100pct = 1 core)' CODEC(ZSTD(1)),
    `max_core_pct` Float32 COMMENT 'Maximum CPU core utilization percentage (100pct = 1 core)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(wallclock_slot_start_date_time)
ORDER BY
    (meta_network_name, wallclock_slot_start_date_time, meta_client_name, pid, client_type)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'CPU utilization data from observoor eBPF agent per sub-slot window';

CREATE TABLE `${NETWORK_NAME}`.observoor_cpu_utilization ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.observoor_cpu_utilization_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    observoor_cpu_utilization_local,
    cityHash64(wallclock_slot_start_date_time, meta_client_name)
);
