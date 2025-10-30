CREATE TABLE `${NETWORK_NAME}`.fct_data_column_availability_by_epoch_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'Earliest slot start time in this epoch (used for CBT position tracking)' CODEC(DoubleDelta, ZSTD(1)),
    `column_index` UInt64 COMMENT 'Column index (0-127)' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Number of slots in this epoch aggregation' CODEC(ZSTD(1)),
    `total_probe_count` UInt64 COMMENT 'Total probe count across all slots' CODEC(DoubleDelta, ZSTD(1)),
    `total_success_count` UInt64 COMMENT 'Total successful probes across all slots' CODEC(DoubleDelta, ZSTD(1)),
    `total_failure_count` UInt64 COMMENT 'Total failed probes across all slots (result = failure)' CODEC(DoubleDelta, ZSTD(1)),
    `total_missing_count` UInt64 COMMENT 'Total missing probes across all slots (result = missing)' CODEC(DoubleDelta, ZSTD(1)),
    `avg_availability_pct` Float64 COMMENT 'Availability percentage calculated from total counts (total_success_count / total_probe_count * 100, rounded to 2 decimal places)' CODEC(ZSTD(1)),
    `min_availability_pct` Float64 COMMENT 'Minimum availability percentage across slots (rounded to 2 decimal places)' CODEC(ZSTD(1)),
    `max_availability_pct` Float64 COMMENT 'Maximum availability percentage across slots (rounded to 2 decimal places)' CODEC(ZSTD(1)),
    `min_response_time_ms` UInt32 COMMENT 'Minimum response time in milliseconds for successful probes only (rounded to whole number)' CODEC(ZSTD(1)),
    `avg_p50_response_time_ms` UInt32 COMMENT 'Average of p50 response times across slots for successful probes only (rounded to whole number)' CODEC(ZSTD(1)),
    `avg_p95_response_time_ms` UInt32 COMMENT 'Average of p95 response times across slots for successful probes only (rounded to whole number)' CODEC(ZSTD(1)),
    `avg_p99_response_time_ms` UInt32 COMMENT 'Average of p99 response times across slots for successful probes only (rounded to whole number)' CODEC(ZSTD(1)),
    `max_response_time_ms` UInt32 COMMENT 'Maximum response time in milliseconds for successful probes only (rounded to whole number)' CODEC(ZSTD(1)),
    `max_blob_count` UInt16 COMMENT 'Maximum blob count observed in this epoch' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(epoch_start_date_time)
ORDER BY
    (`epoch_start_date_time`, `column_index`)
SETTINGS
  deduplicate_merge_projection_mode = 'rebuild',
  min_age_to_force_merge_seconds = 384,
  min_age_to_force_merge_on_partition_only=false
COMMENT 'Data column availability by epoch and column index';

CREATE TABLE `${NETWORK_NAME}`.fct_data_column_availability_by_epoch ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_data_column_availability_by_epoch_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_data_column_availability_by_epoch_local,
    cityHash64(`epoch_start_date_time`, `column_index`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_data_column_availability_by_epoch_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_epoch_column
(
    SELECT *
    ORDER BY (`epoch`, `column_index`)
);
