CREATE TABLE `${NETWORK_NAME}`.int_execution_state_size_local ON CLUSTER '{cluster}' (
  `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' Codec(DoubleDelta, ZSTD(1)),
  `block_number` UInt64 COMMENT 'Block number at which the state size was measured' Codec(DoubleDelta, ZSTD(1)),
  `state_root` FixedString(66) COMMENT 'State root hash of the execution layer at this block' Codec(ZSTD(1)),
  `parent_state_root` FixedString(66) COMMENT 'State root hash of the execution layer at the parent block' Codec(ZSTD(1)),
  `accounts` UInt64 COMMENT 'Total number of accounts in the state' Codec(ZSTD(1)),
  `account_bytes` UInt64 COMMENT 'Total bytes used by account data' Codec(ZSTD(1)),
  `account_trienodes` UInt64 COMMENT 'Number of trie nodes in the account trie' Codec(ZSTD(1)),
  `account_trienode_bytes` UInt64 COMMENT 'Total bytes used by account trie nodes' Codec(ZSTD(1)),
  `contract_codes` UInt64 COMMENT 'Total number of contract codes stored' Codec(ZSTD(1)),
  `contract_code_bytes` UInt64 COMMENT 'Total bytes used by contract code' Codec(ZSTD(1)),
  `storages` UInt64 COMMENT 'Total number of storage slots in the state' Codec(ZSTD(1)),
  `storage_bytes` UInt64 COMMENT 'Total bytes used by storage data' Codec(ZSTD(1)),
  `storage_trienodes` UInt64 COMMENT 'Number of trie nodes in the storage trie' Codec(ZSTD(1)),
  `storage_trienode_bytes` UInt64 COMMENT 'Total bytes used by storage trie nodes' Codec(ZSTD(1)),
) ENGINE = ReplicatedReplacingMergeTree(
  '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
  '{replica}',
  `updated_date_time`
)
PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, state_root)
COMMENT 'Contains execution layer state size metrics including account, contract code, and storage data measurements at specific block heights.';

CREATE TABLE `${NETWORK_NAME}`.int_execution_state_size ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_execution_state_size_local
ENGINE = Distributed(
  '{cluster}',
  `${NETWORK_NAME}`,
  int_execution_state_size_local,
  cityHash64(
    block_number,
    state_root)
);
