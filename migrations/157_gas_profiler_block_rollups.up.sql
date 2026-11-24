CREATE TABLE int_block_opcode_resource_gas_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `count` UInt64 COMMENT 'Number of times this opcode was executed in the block' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Total gas consumed by this opcode in the block' CODEC(ZSTD(1)),
    `gas_compute` UInt64 COMMENT 'Gas attributed to pure computation' CODEC(ZSTD(1)),
    `gas_memory` UInt64 COMMENT 'Gas attributed to memory expansion' CODEC(ZSTD(1)),
    `gas_address_access` UInt64 COMMENT 'Gas attributed to cold address and storage access (EIP-2929)' CODEC(ZSTD(1)),
    `gas_state_growth` UInt64 COMMENT 'Gas attributed to state growth (new storage slots, contract creation)' CODEC(ZSTD(1)),
    `gas_history` UInt64 COMMENT 'Gas attributed to history and log data storage' CODEC(ZSTD(1)),
    `gas_bloom_topics` UInt64 COMMENT 'Gas attributed to bloom filter topic indexing' CODEC(ZSTD(1)),
    `gas_block_size` UInt64 COMMENT 'Gas attributed to block size (always 0 at opcode level)' CODEC(ZSTD(1)),
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600)
ORDER BY (block_number, opcode, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-opcode resource gas per block, summed over every transaction and call frame in the block';

CREATE TABLE int_block_opcode_resource_gas ON CLUSTER '{cluster}' AS int_block_opcode_resource_gas_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_block_opcode_resource_gas_local,
    cityHash64(block_number)
);

CREATE TABLE int_block_transaction_summary_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'Position of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),
    `receipt_gas_used` Nullable(UInt64) COMMENT 'Gas used from the transaction receipt, the total gas the transaction used' CODEC(ZSTD(1)),
    `intrinsic_gas` Nullable(UInt64) COMMENT 'Intrinsic transaction cost (21000 plus calldata). Null for failed transactions' CODEC(ZSTD(1)),
    `gas_refund` UInt64 COMMENT 'Gas refunded to the transaction, 0 for failed transactions' CODEC(ZSTD(1)),
    `evm_gas` UInt64 COMMENT 'EVM execution gas of the whole call tree (the root frame cumulative gas)' CODEC(ZSTD(1)),
    `call_frame_count` UInt32 COMMENT 'Number of call frames in the transaction, including the root frame' CODEC(ZSTD(1)),
    `max_depth` UInt32 COMMENT 'Deepest call depth in the transaction (0 = root only)' CODEC(ZSTD(1)),
    `has_errors` Bool COMMENT 'Whether the root frame had an opcode that resulted in an error' CODEC(ZSTD(1)),
    `root_call_type` LowCardinality(String) COMMENT 'How the transaction started: CALL, CREATE, or the root frame call type' CODEC(ZSTD(1)),
    `target_address` Nullable(String) COMMENT 'The address the transaction called (hex encoded with 0x prefix), null for contract creation' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600)
ORDER BY (block_number, transaction_hash)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'One row per transaction in a block with its call tree summarised: receipt and intrinsic gas from the root frame, and the frame count, deepest call and target from the whole tree';

CREATE TABLE int_block_transaction_summary ON CLUSTER '{cluster}' AS int_block_transaction_summary_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_block_transaction_summary_local,
    cityHash64(block_number)
);

CREATE TABLE int_block_call_type_gas_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `call_type` LowCardinality(String) COMMENT 'The call opcode (CALL, DELEGATECALL, STATICCALL, CALLCODE, CREATE, CREATE2)',
    `call_count` UInt32 COMMENT 'Number of internal call frames of this type in the block' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Gas used by those frames themselves, excluding their child frames' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600)
ORDER BY (block_number, call_type)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Internal calls per call type in a block, every call frame except the transaction root frames';

CREATE TABLE int_block_call_type_gas ON CLUSTER '{cluster}' AS int_block_call_type_gas_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_block_call_type_gas_local,
    cityHash64(block_number)
);

CREATE TABLE int_block_contract_gas_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `target_address` String COMMENT 'The contract address (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `call_types` Array(LowCardinality(String)) COMMENT 'The call types used to call the contract in the block, a transaction calling it directly counts as CALL' CODEC(ZSTD(1)),
    `first_transaction_hash` FixedString(66) COMMENT 'The first transaction in the block that called the contract' CODEC(ZSTD(1)),
    `call_count` UInt32 COMMENT 'Number of call frames that targeted the contract in the block' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Gas used by those frames themselves, excluding their child frames' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600)
ORDER BY (block_number, target_address)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Gas each contract used in a block, summed over every call frame that targeted it, root frames included. Contracts that used no gas are left out';

CREATE TABLE int_block_contract_gas ON CLUSTER '{cluster}' AS int_block_contract_gas_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_block_contract_gas_local,
    cityHash64(block_number)
);

CREATE TABLE int_block_transaction_contract_gas_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `target_address` String COMMENT 'The contract address (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'Position of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),
    `call_count` UInt32 COMMENT 'Number of call frames in the transaction that targeted the contract' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Gas used by those frames themselves, excluding their child frames' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600)
ORDER BY (block_number, transaction_hash, target_address)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Gas each contract used within each transaction of a block, the flows from transactions to contracts. Contracts that used no gas in the transaction are left out';

CREATE TABLE int_block_transaction_contract_gas ON CLUSTER '{cluster}' AS int_block_transaction_contract_gas_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_block_transaction_contract_gas_local,
    cityHash64(block_number)
);
