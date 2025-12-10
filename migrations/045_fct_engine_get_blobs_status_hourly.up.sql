CREATE TABLE `${NETWORK_NAME}`.fct_engine_get_blobs_status_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `is_reference_node` Bool COMMENT 'Whether this observation is from a reference node (controlled fleet with 7870 in name)' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Number of slots in this hour aggregation' CODEC(ZSTD(1)),
    `observation_count` UInt64 COMMENT 'Total number of observations in this hour' CODEC(ZSTD(1)),
    `success_count` UInt64 COMMENT 'Number of observations with SUCCESS status' CODEC(ZSTD(1)),
    `partial_count` UInt64 COMMENT 'Number of observations with PARTIAL status' CODEC(ZSTD(1)),
    `empty_count` UInt64 COMMENT 'Number of observations with EMPTY status' CODEC(ZSTD(1)),
    `unsupported_count` UInt64 COMMENT 'Number of observations with UNSUPPORTED status' CODEC(ZSTD(1)),
    `error_count` UInt64 COMMENT 'Number of observations with ERROR status' CODEC(ZSTD(1)),
    `success_pct` Float64 COMMENT 'Percentage of observations with SUCCESS status' CODEC(ZSTD(1)),
    `avg_duration_ms` UInt64 COMMENT 'Average duration of engine_getBlobs calls in milliseconds' CODEC(ZSTD(1)),
    `avg_p50_duration_ms` UInt64 COMMENT 'Average of median durations across slots in milliseconds' CODEC(ZSTD(1)),
    `avg_p95_duration_ms` UInt64 COMMENT 'Average of p95 durations across slots in milliseconds' CODEC(ZSTD(1)),
    `max_duration_ms` UInt64 COMMENT 'Maximum duration of engine_getBlobs calls in milliseconds' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time, is_reference_node)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly aggregated engine_getBlobs status distribution and duration statistics';

CREATE TABLE `${NETWORK_NAME}`.fct_engine_get_blobs_status_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_engine_get_blobs_status_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_engine_get_blobs_status_hourly_local,
    cityHash64(hour_start_date_time, is_reference_node)
);
