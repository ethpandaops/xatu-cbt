-- Add resource gas building block columns to int_transaction_call_frame_opcode_gas.
-- These columns enable downstream SQL to compute memory expansion gas and cold access gas
-- without needing per-opcode structlog data.

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `memory_words_sum_before` UInt64 DEFAULT 0 COMMENT 'SUM(ceil(memory_bytes/32)) before each opcode executes.' CODEC(ZSTD(1)) AFTER `error_count`,
    ADD COLUMN IF NOT EXISTS `memory_words_sum_after` UInt64 DEFAULT 0 COMMENT 'SUM(ceil(memory_bytes/32)) after each opcode executes.' CODEC(ZSTD(1)) AFTER `memory_words_sum_before`,
    ADD COLUMN IF NOT EXISTS `memory_words_sq_sum_before` UInt64 DEFAULT 0 COMMENT 'SUM(words_before²).' CODEC(ZSTD(1)) AFTER `memory_words_sum_after`,
    ADD COLUMN IF NOT EXISTS `memory_words_sq_sum_after` UInt64 DEFAULT 0 COMMENT 'SUM(words_after²).' CODEC(ZSTD(1)) AFTER `memory_words_sq_sum_before`,
    ADD COLUMN IF NOT EXISTS `memory_expansion_gas` UInt64 DEFAULT 0 COMMENT 'SUM(memory_expansion_gas). Exact per-opcode memory expansion cost.' CODEC(ZSTD(1)) AFTER `memory_words_sq_sum_after`,
    ADD COLUMN IF NOT EXISTS `cold_access_count` UInt64 DEFAULT 0 COMMENT 'Number of cold storage/account accesses (EIP-2929).' CODEC(ZSTD(1)) AFTER `memory_expansion_gas`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `memory_words_sum_before` UInt64 DEFAULT 0 COMMENT 'SUM(ceil(memory_bytes/32)) before each opcode executes.' AFTER `error_count`,
    ADD COLUMN IF NOT EXISTS `memory_words_sum_after` UInt64 DEFAULT 0 COMMENT 'SUM(ceil(memory_bytes/32)) after each opcode executes.' AFTER `memory_words_sum_before`,
    ADD COLUMN IF NOT EXISTS `memory_words_sq_sum_before` UInt64 DEFAULT 0 COMMENT 'SUM(words_before²).' AFTER `memory_words_sum_after`,
    ADD COLUMN IF NOT EXISTS `memory_words_sq_sum_after` UInt64 DEFAULT 0 COMMENT 'SUM(words_after²).' AFTER `memory_words_sq_sum_before`,
    ADD COLUMN IF NOT EXISTS `memory_expansion_gas` UInt64 DEFAULT 0 COMMENT 'SUM(memory_expansion_gas). Exact per-opcode memory expansion cost.' AFTER `memory_words_sq_sum_after`,
    ADD COLUMN IF NOT EXISTS `cold_access_count` UInt64 DEFAULT 0 COMMENT 'Number of cold storage/account accesses (EIP-2929).' AFTER `memory_expansion_gas`;

-- =============================================================================
-- int_transaction_call_frame_opcode_resource_gas
-- =============================================================================
-- Per-frame, per-opcode resource gas decomposition.
-- Splits EVM gas into 7 resource categories: compute, memory, address_access,
-- state_growth, history, bloom_topics, block_size.

CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_resource_gas_local ON CLUSTER '{cluster}' (
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
    `gas` UInt64 COMMENT 'Total gas consumed by this opcode in this frame' CODEC(ZSTD(1)),

    -- Resource gas decomposition (sum of all 7 = gas)
    `gas_compute` UInt64 COMMENT 'Gas attributed to pure computation' CODEC(ZSTD(1)),
    `gas_memory` UInt64 COMMENT 'Gas attributed to memory expansion' CODEC(ZSTD(1)),
    `gas_address_access` UInt64 COMMENT 'Gas attributed to cold address/storage access (EIP-2929)' CODEC(ZSTD(1)),
    `gas_state_growth` UInt64 COMMENT 'Gas attributed to state growth (new storage slots, contract creation)' CODEC(ZSTD(1)),
    `gas_history` UInt64 COMMENT 'Gas attributed to history/log data storage' CODEC(ZSTD(1)),
    `gas_bloom_topics` UInt64 COMMENT 'Gas attributed to bloom filter topic indexing' CODEC(ZSTD(1)),
    `gas_block_size` UInt64 COMMENT 'Gas attributed to block size (always 0 at opcode level)' CODEC(ZSTD(1)),

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
COMMENT 'Per-frame per-opcode resource gas decomposition into 7 categories.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_resource_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_call_frame_opcode_resource_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_transaction_call_frame_opcode_resource_gas_local,
    cityHash64(block_number, transaction_hash)
);

-- Projection for frame-first queries (e.g., "resource breakdown for frame 15")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_resource_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_frame (
    SELECT *
    ORDER BY (transaction_hash, call_frame_id, opcode)
);

-- Projection for opcode-first queries (e.g., "how much memory gas did MLOAD use?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_resource_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number)
);

-- =============================================================================
-- int_transaction_resource_gas
-- =============================================================================
-- Per-transaction resource gas totals including intrinsic gas decomposition.

CREATE TABLE `${NETWORK_NAME}`.int_transaction_resource_gas_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Transaction identifiers
    `block_number` UInt64 COMMENT 'The block number containing the transaction' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'The index of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

    -- Resource gas decomposition
    `gas_compute` UInt64 COMMENT 'Total compute gas (EVM execution + intrinsic compute)' CODEC(ZSTD(1)),
    `gas_memory` UInt64 COMMENT 'Total memory expansion gas' CODEC(ZSTD(1)),
    `gas_address_access` UInt64 COMMENT 'Total cold address/storage access gas' CODEC(ZSTD(1)),
    `gas_state_growth` UInt64 COMMENT 'Total state growth gas' CODEC(ZSTD(1)),
    `gas_history` UInt64 COMMENT 'Total history/log data gas' CODEC(ZSTD(1)),
    `gas_bloom_topics` UInt64 COMMENT 'Total bloom filter topic gas' CODEC(ZSTD(1)),
    `gas_block_size` UInt64 COMMENT 'Total block size gas (from intrinsic calldata)' CODEC(ZSTD(1)),
    `gas_refund` UInt64 COMMENT 'Gas refund from SSTORE operations' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-transaction resource gas totals including intrinsic gas decomposition.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_transaction_resource_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_resource_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_transaction_resource_gas_local,
    cityHash64(block_number, transaction_hash)
);

-- Projection for transaction lookups without block_number
ALTER TABLE `${NETWORK_NAME}`.int_transaction_resource_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_transaction (
    SELECT *
    ORDER BY (transaction_hash)
);

-- =============================================================================
-- int_block_resource_gas
-- =============================================================================
-- Per-block resource gas totals.

CREATE TABLE `${NETWORK_NAME}`.int_block_resource_gas_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Block identifier
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),

    -- Resource gas decomposition
    `gas_compute` UInt64 COMMENT 'Total compute gas across all transactions' CODEC(ZSTD(1)),
    `gas_memory` UInt64 COMMENT 'Total memory expansion gas across all transactions' CODEC(ZSTD(1)),
    `gas_address_access` UInt64 COMMENT 'Total cold address/storage access gas across all transactions' CODEC(ZSTD(1)),
    `gas_state_growth` UInt64 COMMENT 'Total state growth gas across all transactions' CODEC(ZSTD(1)),
    `gas_history` UInt64 COMMENT 'Total history/log data gas across all transactions' CODEC(ZSTD(1)),
    `gas_bloom_topics` UInt64 COMMENT 'Total bloom filter topic gas across all transactions' CODEC(ZSTD(1)),
    `gas_block_size` UInt64 COMMENT 'Total block size gas across all transactions' CODEC(ZSTD(1)),
    `gas_refund` UInt64 COMMENT 'Total gas refund across all transactions' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-block resource gas totals. Derived from int_transaction_resource_gas.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_block_resource_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_block_resource_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_block_resource_gas_local,
    cityHash64(block_number)
);
