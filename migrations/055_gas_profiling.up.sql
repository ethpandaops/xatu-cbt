-- =============================================================================
-- int_transaction_opcode_gas
-- =============================================================================

-- Local table for opcode-level gas aggregation per transaction
CREATE TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Transaction identifiers
    `block_number` UInt64 COMMENT 'The block number containing the transaction' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'The index of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

    -- Opcode aggregation
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `count` UInt64 COMMENT 'Number of times this opcode was executed in the transaction' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Gas consumed by this opcode. sum(gas) = transaction executed gas' CODEC(ZSTD(1)),
    `gas_cumulative` UInt64 COMMENT 'For CALL opcodes: includes all descendant frame gas. For others: same as gas' CODEC(ZSTD(1)),

    -- Call depth metrics
    `min_depth` UInt64 COMMENT 'Minimum call stack depth for this opcode' CODEC(ZSTD(1)),
    `max_depth` UInt64 COMMENT 'Maximum call stack depth for this opcode' CODEC(ZSTD(1)),

    -- Error tracking
    `error_count` UInt64 COMMENT 'Number of times this opcode resulted in an error' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, opcode, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated opcode-level gas usage per transaction. Source: canonical_execution_transaction_structlog';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_opcode_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_transaction_opcode_gas_local,
    cityHash64(block_number, transaction_hash)
);

-- Projection for opcode-first queries (e.g., "how much gas did SLOAD use across blocks?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number, transaction_hash)
);

-- Projection for transaction-first queries (e.g., "what opcodes did this tx use?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_transaction (
    SELECT *
    ORDER BY (transaction_hash, opcode)
);

-- =============================================================================
-- int_transaction_call_frame
-- =============================================================================

CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_local ON CLUSTER '{cluster}' (
  -- Metadata
  `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

  -- Block and transaction identifiers
  `block_number` UInt64 COMMENT 'The block number containing this transaction' CODEC(DoubleDelta, ZSTD(1)),
  `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
  `transaction_index` UInt32 COMMENT 'Position of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

  -- Call frame identifiers
  `call_frame_id` UInt32 COMMENT 'Sequential frame ID within the transaction (0 = root)' CODEC(DoubleDelta, ZSTD(1)),
  `parent_call_frame_id` Nullable(UInt32) COMMENT 'Parent frame ID (NULL for root frame)' CODEC(ZSTD(1)),
  `depth` UInt32 COMMENT 'Call depth (0 = root transaction execution)' CODEC(DoubleDelta, ZSTD(1)),

  -- Call information
  `target_address` Nullable(String) COMMENT 'Contract address being called (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
  `call_type` LowCardinality(String) COMMENT 'Type of call opcode (CALL, DELEGATECALL, STATICCALL, CALLCODE, CREATE, CREATE2)' CODEC(ZSTD(1)),
  `function_selector` Nullable(String) COMMENT 'Function selector (first 4 bytes of call input, hex encoded with 0x prefix). Populated for all frames from traces.' CODEC(ZSTD(1)),

  -- Aggregated metrics
  `opcode_count` UInt64 COMMENT 'Number of opcodes executed in this frame' CODEC(ZSTD(1)),
  `error_count` UInt64 COMMENT 'Number of opcodes that resulted in errors' CODEC(ZSTD(1)),

  -- Gas metrics (see transformation SQL for full gas model explanation)
  -- Receipt gas = (intrinsic + gas_cumulative) - min(gas_refund, (intrinsic + gas_cumulative) / 5)
  `gas` UInt64 COMMENT 'Gas consumed by this frame only, excludes child frames. sum(gas) = EVM execution gas. This is "self" gas in flame graphs.' CODEC(ZSTD(1)),
  `gas_cumulative` UInt64 COMMENT 'Gas consumed by this frame + all descendants. Root frame value = total EVM execution gas.' CODEC(ZSTD(1)),
  `gas_refund` Nullable(UInt64) COMMENT 'Total accumulated refund. Only populated for root frame (refund applied once at tx end).' CODEC(ZSTD(1)),
  `intrinsic_gas` Nullable(UInt64) COMMENT 'Intrinsic tx cost (21000 + calldata). Only populated for root frame (call_frame_id=0).' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
  '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
  '{replica}',
  updated_date_time
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, call_frame_id)
SETTINGS
  deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated call frame activity per transaction for call tree analysis';

CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_call_frame_local
ENGINE = Distributed(
  '{cluster}',
  '${NETWORK_NAME}',
  int_transaction_call_frame_local,
  cityHash64(block_number, transaction_hash)
);

-- Projection for transaction lookups without block_number
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_transaction (
  SELECT *
  ORDER BY (transaction_hash, call_frame_id)
);

-- =============================================================================
-- int_transaction_call_frame_opcode_gas
-- =============================================================================

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

-- =============================================================================
-- dim_function_signature
-- =============================================================================

CREATE TABLE `${NETWORK_NAME}`.dim_function_signature_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `selector` String COMMENT 'Function selector (first 4 bytes of keccak256 hash, hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `name` String COMMENT 'Function signature name (e.g., transfer(address,uint256))' CODEC(ZSTD(1)),
    `has_verified_contract` Bool DEFAULT false COMMENT 'Whether this signature comes from a verified contract source' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PRIMARY KEY (selector)
ORDER BY (selector)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Function signature lookup table populated from Sourcify signature database.';

CREATE TABLE `${NETWORK_NAME}`.dim_function_signature ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.dim_function_signature_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_function_signature_local,
    cityHash64(selector)
);

-- =============================================================================
-- fct_block_opcode_gas
-- =============================================================================

-- Local table for block-level opcode gas aggregation
-- Aggregates opcode gas data from int_transaction_opcode_gas to block level
CREATE TABLE `${NETWORK_NAME}`.fct_block_opcode_gas_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Block identifier
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),

    -- Opcode aggregation
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `count` UInt64 COMMENT 'Total execution count of this opcode across all transactions in the block' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Total gas consumed by this opcode across all transactions in the block' CODEC(ZSTD(1)),

    -- Error tracking
    `error_count` UInt64 COMMENT 'Number of times this opcode resulted in an error across all transactions' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, opcode, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated opcode-level gas usage per block. Derived from int_transaction_opcode_gas.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.fct_block_opcode_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_block_opcode_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_opcode_gas_local,
    cityHash64(block_number)
);

-- Projection for opcode-first queries (e.g., "how much gas did SLOAD use across blocks?")
ALTER TABLE `${NETWORK_NAME}`.fct_block_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number)
);
