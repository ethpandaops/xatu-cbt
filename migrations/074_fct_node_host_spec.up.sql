-- Time-history fact table for node host hardware specifications
CREATE TABLE `${NETWORK_NAME}`.fct_node_host_spec_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `wallclock_slot_start_date_time` DateTime64(3) COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the observoor client that collected the data',
    `meta_network_name` LowCardinality(String) COMMENT 'Ethereum network name',
    `node_class` LowCardinality(String) COMMENT 'Node classification for filtering (e.g. eip7870)',
    `host_id` String COMMENT 'Unique host identifier' CODEC(ZSTD(1)),
    `kernel_release` LowCardinality(String) COMMENT 'OS kernel release version',
    `os_name` LowCardinality(String) COMMENT 'Operating system name',
    `architecture` LowCardinality(String) COMMENT 'CPU architecture (e.g. x86_64, aarch64)',
    `cpu_model` String COMMENT 'CPU model name' CODEC(ZSTD(1)),
    `cpu_vendor` LowCardinality(String) COMMENT 'CPU vendor (e.g. GenuineIntel, AuthenticAMD)',
    `cpu_online_cores` UInt16 COMMENT 'Number of online CPU cores' CODEC(ZSTD(1)),
    `cpu_logical_cores` UInt16 COMMENT 'Number of logical CPU cores' CODEC(ZSTD(1)),
    `cpu_physical_cores` UInt16 COMMENT 'Number of physical CPU cores' CODEC(ZSTD(1)),
    `cpu_performance_cores` UInt16 COMMENT 'Number of performance cores (hybrid CPUs)' CODEC(ZSTD(1)),
    `cpu_efficiency_cores` UInt16 COMMENT 'Number of efficiency cores (hybrid CPUs)' CODEC(ZSTD(1)),
    `cpu_unknown_type_cores` UInt16 COMMENT 'Number of cores with unknown type' CODEC(ZSTD(1)),
    `cpu_core_types` Array(UInt8) COMMENT 'Core type identifiers per core',
    `cpu_core_type_labels` Array(LowCardinality(String)) COMMENT 'Core type labels per core',
    `cpu_max_freq_khz` Array(UInt64) COMMENT 'Maximum frequency per core in kHz',
    `cpu_base_freq_khz` Array(UInt64) COMMENT 'Base frequency per core in kHz',
    `memory_total_bytes` UInt64 COMMENT 'Total system memory in bytes' CODEC(ZSTD(1)),
    `memory_type` LowCardinality(String) COMMENT 'Memory type (e.g. DDR4, DDR5)',
    `memory_speed_mts` UInt32 COMMENT 'Memory speed in MT/s' CODEC(ZSTD(1)),
    `memory_dimm_count` UInt16 COMMENT 'Number of memory DIMMs' CODEC(ZSTD(1)),
    `memory_dimm_sizes_bytes` Array(UInt64) COMMENT 'Size of each DIMM in bytes',
    `memory_dimm_types` Array(LowCardinality(String)) COMMENT 'Type of each DIMM',
    `memory_dimm_speeds_mts` Array(UInt32) COMMENT 'Speed of each DIMM in MT/s',
    `disk_count` UInt16 COMMENT 'Number of disk devices' CODEC(ZSTD(1)),
    `disk_total_bytes` UInt64 COMMENT 'Total disk capacity in bytes' CODEC(ZSTD(1)),
    `disk_names` Array(String) COMMENT 'Device names of each disk',
    `disk_models` Array(String) COMMENT 'Model names of each disk',
    `disk_sizes_bytes` Array(UInt64) COMMENT 'Size of each disk in bytes',
    `disk_rotational` Array(UInt8) COMMENT 'Whether each disk is rotational (1) or SSD (0)'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(wallclock_slot_start_date_time)
ORDER BY
    (wallclock_slot_start_date_time, meta_client_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Node host hardware specifications over time with node classification';

CREATE TABLE `${NETWORK_NAME}`.fct_node_host_spec ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_node_host_spec_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_node_host_spec_local,
    cityHash64(wallclock_slot_start_date_time, meta_client_name)
);
