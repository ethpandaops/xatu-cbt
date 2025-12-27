-- ============================================================================
-- Migration: Track Contract Creation and SELFDESTRUCT Events
-- ============================================================================
-- int_contract_creation: Contract creation events with projection for efficient
--   address lookups (used by int_contract_selfdestruct)
--
-- int_contract_selfdestruct: Tracks when SELFDESTRUCT is called and determines
--   if storage was cleared based on EIP-6780 rules.
--
-- EIP-6780 (Shanghai):
--   - Pre-Shanghai: SELFDESTRUCT always clears all storage slots
--   - Post-Shanghai: SELFDESTRUCT only clears storage if contract was created
--     in the same transaction
-- ============================================================================

-- ============================================================================
-- int_contract_creation
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_creation_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'Block where contract was created' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` String COMMENT 'Transaction hash' CODEC(ZSTD(1)),
    `internal_index` UInt32 COMMENT 'Position within transaction' CODEC(DoubleDelta, ZSTD(1)),
    `contract_address` String COMMENT 'Address of created contract' CODEC(ZSTD(1)),
    `deployer` String COMMENT 'Address that deployed the contract' CODEC(ZSTD(1)),
    `factory` String COMMENT 'Factory contract address if applicable' CODEC(ZSTD(1)),
    `init_code_hash` String COMMENT 'Hash of the initialization code' CODEC(ZSTD(1)),
    PROJECTION by_contract_address (
        SELECT * ORDER BY (contract_address, block_number, transaction_hash)
    )
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, contract_address, transaction_hash)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Contract creation events with projection for efficient address lookups';

CREATE TABLE `${NETWORK_NAME}`.int_contract_creation ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_creation_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_creation_local,
    cityHash64(block_number, contract_address)
);

-- ============================================================================
-- int_contract_selfdestruct
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_selfdestruct_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'Block where SELFDESTRUCT occurred' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` String COMMENT 'Transaction hash' CODEC(ZSTD(1)),
    `transaction_index` UInt16 COMMENT 'Position in block' CODEC(DoubleDelta, ZSTD(1)),
    `internal_index` UInt32 COMMENT 'Position within transaction traces' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'Contract that was destroyed' CODEC(ZSTD(1)),
    `beneficiary` String COMMENT 'Address receiving the ETH' CODEC(ZSTD(1)),
    `value_transferred` UInt256 COMMENT 'Amount of ETH sent to beneficiary' CODEC(ZSTD(1)),
    `ephemeral` Bool COMMENT 'True if contract was created and destroyed in the same transaction - storage always cleared per EIP-6780' CODEC(ZSTD(1)),
    `storage_cleared` Bool COMMENT 'True if storage was cleared (pre-Shanghai OR ephemeral)' CODEC(ZSTD(1)),
    `creation_block` Nullable(UInt32) COMMENT 'Block where contract was created (if known)' CODEC(ZSTD(1)),
    `creation_transaction_hash` Nullable(String) COMMENT 'Transaction that created the contract (if known)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, transaction_index, internal_index, address)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'SELFDESTRUCT operations with EIP-6780 storage clearing implications';

CREATE TABLE `${NETWORK_NAME}`.int_contract_selfdestruct ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_selfdestruct_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_selfdestruct_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_selfdestruct_diffs
-- ============================================================================
-- Synthetic storage diffs for selfdestructs that clear storage.
-- Matches canonical_execution_storage_diffs schema for union compatibility in
-- int_storage_slot_diff.
--
-- This table records the implicit clearing of storage slots when:
--   - Pre-Shanghai: All selfdestructs clear storage
--   - Post-Shanghai (EIP-6780): Only ephemeral contracts clear storage
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_selfdestruct_diffs_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'Block where SELFDESTRUCT occurred' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'Transaction index within the block' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'Transaction hash of the SELFDESTRUCT' CODEC(ZSTD(1)),
    `internal_index` UInt32 COMMENT 'Internal index of the SELFDESTRUCT trace' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'Contract address that was selfdestructed' CODEC(ZSTD(1)),
    `slot` String COMMENT 'Storage slot key being cleared' CODEC(ZSTD(1)),
    `from_value` String COMMENT 'Value before clearing (last known value)' CODEC(ZSTD(1)),
    `to_value` String COMMENT 'Value after clearing (always 0x00...00)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 1000000)
ORDER BY (block_number, transaction_hash, internal_index, slot)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Synthetic storage diffs for selfdestructs that clear all storage slots';

CREATE TABLE `${NETWORK_NAME}`.int_storage_selfdestruct_diffs ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_selfdestruct_diffs_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_selfdestruct_diffs_local,
    cityHash64(block_number, address)
);
