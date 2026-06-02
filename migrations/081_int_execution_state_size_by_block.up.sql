-- Cumulative execution layer state size per block (incremental, reconstructed
-- from execution_state_size_delta). Replaces the deprecated execution_state_size
-- snapshot table as the source for the hourly/daily aggregations.
CREATE TABLE `${NETWORK_NAME}`.int_execution_state_size_by_block_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt64 COMMENT 'Block number at which the cumulative state size is measured' CODEC(DoubleDelta, ZSTD(1)),
    `accounts` UInt64 COMMENT 'Cumulative total number of accounts in the state' CODEC(ZSTD(1)),
    `account_bytes` UInt64 COMMENT 'Cumulative total bytes used by account data' CODEC(ZSTD(1)),
    `account_trienodes` UInt64 COMMENT 'Cumulative number of trie nodes in the account trie' CODEC(ZSTD(1)),
    `account_trienode_bytes` UInt64 COMMENT 'Cumulative total bytes used by account trie nodes' CODEC(ZSTD(1)),
    `contract_codes` UInt64 COMMENT 'Cumulative total number of contract codes stored' CODEC(ZSTD(1)),
    `contract_code_bytes` UInt64 COMMENT 'Cumulative total bytes used by contract code' CODEC(ZSTD(1)),
    `storages` UInt64 COMMENT 'Cumulative total number of storage slots in the state' CODEC(ZSTD(1)),
    `storage_bytes` UInt64 COMMENT 'Cumulative total bytes used by storage data' CODEC(ZSTD(1)),
    `storage_trienodes` UInt64 COMMENT 'Cumulative number of trie nodes in the storage trie' CODEC(ZSTD(1)),
    `storage_trienode_bytes` UInt64 COMMENT 'Cumulative total bytes used by storage trie nodes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (`block_number`)
COMMENT 'Cumulative execution layer state size per block, reconstructed from execution_state_size_delta. Drop-in replacement for the deprecated execution_state_size snapshot table.';

CREATE TABLE `${NETWORK_NAME}`.int_execution_state_size_by_block ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_execution_state_size_by_block_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_execution_state_size_by_block_local,
    cityHash64(`block_number`)
);
