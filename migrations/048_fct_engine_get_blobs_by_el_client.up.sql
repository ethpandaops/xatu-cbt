CREATE TABLE `${NETWORK_NAME}`.fct_engine_get_blobs_by_el_client_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number of the beacon block being reconstructed' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` FixedString(66) COMMENT 'Root of the beacon block (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Consensus client implementation (e.g., lighthouse, prysm, teku)',
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation (e.g., Geth, Nethermind, Besu, Reth)',
    `is_reference_node` Bool COMMENT 'Whether this observation is from a reference node (controlled fleet with 7870 in name)' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of observations for this EL client' CODEC(ZSTD(1)),
    `unique_node_count` UInt32 COMMENT 'Number of unique nodes with this EL client' CODEC(ZSTD(1)),
    `max_requested_count` UInt32 COMMENT 'Maximum number of versioned hashes requested' CODEC(ZSTD(1)),
    `avg_returned_count` Float64 COMMENT 'Average number of non-null blobs returned' CODEC(ZSTD(1)),
    `success_count` UInt32 COMMENT 'Number of observations with SUCCESS status' CODEC(ZSTD(1)),
    `partial_count` UInt32 COMMENT 'Number of observations with PARTIAL status' CODEC(ZSTD(1)),
    `empty_count` UInt32 COMMENT 'Number of observations with EMPTY status' CODEC(ZSTD(1)),
    `unsupported_count` UInt32 COMMENT 'Number of observations with UNSUPPORTED status' CODEC(ZSTD(1)),
    `error_count` UInt32 COMMENT 'Number of observations with ERROR status' CODEC(ZSTD(1)),
    `success_pct` Float64 COMMENT 'Percentage of observations with SUCCESS status' CODEC(ZSTD(1)),
    `avg_duration_ms` UInt64 COMMENT 'Average duration of engine_getBlobs calls in milliseconds' CODEC(ZSTD(1)),
    `median_duration_ms` UInt64 COMMENT 'Median duration of engine_getBlobs calls in milliseconds' CODEC(ZSTD(1)),
    `min_duration_ms` UInt64 COMMENT 'Minimum duration of engine_getBlobs calls in milliseconds' CODEC(ZSTD(1)),
    `max_duration_ms` UInt64 COMMENT 'Maximum duration of engine_getBlobs calls in milliseconds' CODEC(ZSTD(1)),
    `p95_duration_ms` UInt64 COMMENT '95th percentile duration of engine_getBlobs calls in milliseconds' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (slot_start_date_time, block_root, meta_client_implementation, meta_execution_implementation, is_reference_node)
COMMENT 'engine_getBlobs observations aggregated by execution client for EL comparison';

CREATE TABLE `${NETWORK_NAME}`.fct_engine_get_blobs_by_el_client ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_engine_get_blobs_by_el_client_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_engine_get_blobs_by_el_client_local,
    cityHash64(slot_start_date_time, block_root, meta_client_implementation, meta_execution_implementation, is_reference_node)
);
