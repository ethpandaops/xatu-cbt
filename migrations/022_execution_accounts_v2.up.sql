CREATE TABLE `${NETWORK_NAME}`.int_address_diffs_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the diffs' CODEC(ZSTD(1)),
    `tx_count` UInt32 COMMENT 'The number of transactions with diffs for this address in the block' CODEC(ZSTD(1)),
    `last_tx_index` UInt32 COMMENT 'The last transaction index with diffs for this address in the block' CODEC(ZSTD(1)),
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
) PARTITION BY cityHash64(`address`) % 16
ORDER BY
    (address) COMMENT 'Table for accounts last access data';

CREATE TABLE `${NETWORK_NAME}`.int_address_diffs ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_diffs_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_address_diffs_local,
    cityHash64(`address`)
);

CREATE TABLE `${NETWORK_NAME}`.int_address_reads_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the reads' CODEC(ZSTD(1)),
    `tx_count` UInt32 COMMENT 'The number of reads for this address in this block' CODEC(ZSTD(1)),
    `last_tx_index` UInt32 COMMENT 'The last transaction index with diffs for this address in the block' CODEC(ZSTD(1)),
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}'
) PARTITION BY cityHash64(`address`) % 16
ORDER BY
    (address, block_number) COMMENT 'Table for accounts reads data';

CREATE TABLE `${NETWORK_NAME}`.int_address_reads ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_reads_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_address_reads_local,
    cityHash64(`address`)
);

CREATE TABLE `${NETWORK_NAME}`.int_pre_6780_accounts_destructs_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the self-destructs' CODEC(ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash' CODEC(ZSTD(1)),
    `transaction_index` UInt64 COMMENT 'The transaction index' CODEC(DoubleDelta, ZSTD(1)),
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}'
) PARTITION BY cityHash64(`address`) % 16
ORDER BY
    (address, block_number, transaction_hash) COMMENT 'Table for accounts self-destructs data pre-6780 (Dencun fork)';

CREATE TABLE `${NETWORK_NAME}`.int_pre_6780_accounts_destructs ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_pre_6780_accounts_destructs_local ENGINE = Distributed(
    '{cluster}',
    `${NETWORK_NAME}`,
    int_pre_6780_accounts_destructs_local,
    cityHash64(`address`)
);