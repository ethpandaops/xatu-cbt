CREATE TABLE `${NETWORK_NAME}`.fct_engine_new_payload_by_slot_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number of the beacon block containing the payload' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` FixedString(66) COMMENT 'Root of the beacon block (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'Execution block hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `block_number` UInt64 COMMENT 'Execution block number' CODEC(DoubleDelta, ZSTD(1)),
    `proposer_index` UInt32 COMMENT 'Validator index of the block proposer' CODEC(ZSTD(1)),
    `gas_used` UInt64 COMMENT 'Total gas used by all transactions in the block' CODEC(ZSTD(1)),
    `gas_limit` UInt64 COMMENT 'Gas limit of the block' CODEC(ZSTD(1)),
    `tx_count` UInt32 COMMENT 'Number of transactions in the block' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'Number of blobs in the block' CODEC(ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification for grouping observations (e.g., eip7870-block-builder, or empty for general nodes)' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of observations for this slot/block' CODEC(ZSTD(1)),
    `unique_node_count` UInt32 COMMENT 'Number of unique nodes that observed this block' CODEC(ZSTD(1)),
    `valid_count` UInt32 COMMENT 'Number of observations with VALID status' CODEC(ZSTD(1)),
    `invalid_count` UInt32 COMMENT 'Number of observations with INVALID status' CODEC(ZSTD(1)),
    `syncing_count` UInt32 COMMENT 'Number of observations with SYNCING status' CODEC(ZSTD(1)),
    `accepted_count` UInt32 COMMENT 'Number of observations with ACCEPTED status' CODEC(ZSTD(1)),
    `invalid_block_hash_count` UInt32 COMMENT 'Number of observations with INVALID_BLOCK_HASH status' CODEC(ZSTD(1)),
    `error_count` UInt32 COMMENT 'Number of observations with ERROR status' CODEC(ZSTD(1)),
    `valid_pct` Float64 COMMENT 'Percentage of observations with VALID status' CODEC(ZSTD(1)),
    `avg_duration_ms` UInt64 COMMENT 'Average duration of engine_newPayload calls in milliseconds' CODEC(ZSTD(1)),
    `median_duration_ms` UInt64 COMMENT 'Median duration of engine_newPayload calls in milliseconds' CODEC(ZSTD(1)),
    `min_duration_ms` UInt64 COMMENT 'Minimum duration of engine_newPayload calls in milliseconds' CODEC(ZSTD(1)),
    `max_duration_ms` UInt64 COMMENT 'Maximum duration of engine_newPayload calls in milliseconds' CODEC(ZSTD(1)),
    `p95_duration_ms` UInt64 COMMENT '95th percentile duration of engine_newPayload calls in milliseconds' CODEC(ZSTD(1)),
    `p99_duration_ms` UInt64 COMMENT '99th percentile duration of engine_newPayload calls in milliseconds' CODEC(ZSTD(1)),
    `unique_cl_implementation_count` UInt32 COMMENT 'Number of unique CL client implementations observing this block' CODEC(ZSTD(1)),
    `unique_el_implementation_count` UInt32 COMMENT 'Number of unique EL client implementations observing this block' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (slot_start_date_time, block_hash, node_class)
COMMENT 'Slot-level aggregated engine_newPayload observations with status distribution and duration statistics';

CREATE TABLE `${NETWORK_NAME}`.fct_engine_new_payload_by_slot ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_engine_new_payload_by_slot_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_engine_new_payload_by_slot_local,
    cityHash64(slot_start_date_time, block_hash, node_class)
);
