CREATE TABLE `${NETWORK_NAME}`.fct_engine_get_blobs_duration_chunked_50ms_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number of the beacon block being reconstructed' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` FixedString(66) COMMENT 'Root of the beacon block (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `is_reference_node` Bool COMMENT 'Whether this observation is from a reference node (controlled fleet with 7870 in name)' CODEC(ZSTD(1)),
    `chunk_duration_ms` Int64 COMMENT 'Duration bucket in 50ms chunks (0, 50, 100, 150, ...)' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of observations in this duration chunk' CODEC(ZSTD(1)),
    `success_count` UInt32 COMMENT 'Number of SUCCESS status observations in this chunk' CODEC(ZSTD(1)),
    `partial_count` UInt32 COMMENT 'Number of PARTIAL status observations in this chunk' CODEC(ZSTD(1)),
    `empty_count` UInt32 COMMENT 'Number of EMPTY status observations in this chunk' CODEC(ZSTD(1)),
    `error_count` UInt32 COMMENT 'Number of ERROR or UNSUPPORTED status observations in this chunk' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (slot_start_date_time, block_root, is_reference_node, chunk_duration_ms)
COMMENT 'Fine-grained engine_getBlobs duration distribution in 50ms chunks for latency histogram analysis';

CREATE TABLE `${NETWORK_NAME}`.fct_engine_get_blobs_duration_chunked_50ms ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_engine_get_blobs_duration_chunked_50ms_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_engine_get_blobs_duration_chunked_50ms_local,
    cityHash64(slot_start_date_time, block_root, is_reference_node, chunk_duration_ms)
);
