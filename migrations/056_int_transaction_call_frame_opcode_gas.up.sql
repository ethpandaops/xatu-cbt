-- Local table for opcode-level gas aggregation per call frame within transactions
-- This table enables per-frame opcode breakdown, answering "which opcodes did frame N execute?"
CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Transaction identifiers
    `block_number` UInt64 COMMENT 'The block number containing the transaction' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'The index of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

    -- Call frame identifier
    `call_frame_id` UInt32 COMMENT 'Sequential frame ID within transaction (0 = root)' CODEC(DoubleDelta, ZSTD(1)),

    -- Opcode aggregation
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `count` UInt64 COMMENT 'Number of times this opcode was executed in this frame' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Gas consumed by this opcode in this frame. sum(gas) = frame gas' CODEC(ZSTD(1)),
    `gas_cumulative` UInt64 COMMENT 'For CALL opcodes: includes all descendant frame gas. For others: same as gas' CODEC(ZSTD(1)),

    -- Error tracking
    `error_count` UInt64 COMMENT 'Number of times this opcode resulted in an error in this frame' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, call_frame_id, opcode, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated opcode-level gas usage per call frame. Enables per-frame opcode analysis.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_transaction_call_frame_opcode_gas_local,
    cityHash64(block_number, transaction_hash)
);

-- Projection for frame-first queries (e.g., "what opcodes did frame 15 execute?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_frame (
    SELECT *
    ORDER BY (transaction_hash, call_frame_id, opcode)
);

-- Projection for opcode-first queries (e.g., "which frames used SLOAD?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number, transaction_hash, call_frame_id)
);
