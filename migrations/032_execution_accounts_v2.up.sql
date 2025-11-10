CREATE TABLE `${NETWORK_NAME}`.int_address_diffs_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the diffs' CODEC(ZSTD(1)),
    `tx_count` UInt32 COMMENT 'The number of transactions with diffs for this address in the block' CODEC(ZSTD(1)),
    `last_tx_index` UInt32 COMMENT 'The last transaction index with diffs for this address in the block' CODEC(ZSTD(1))
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}'
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
    `last_tx_index` UInt32 COMMENT 'The last transaction index with diffs for this address in the block' CODEC(ZSTD(1))
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
    `transaction_index` UInt64 COMMENT 'The transaction index' CODEC(DoubleDelta, ZSTD(1))
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

CREATE TABLE `${NETWORK_NAME}`.int_post_6780_accounts_destructs_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash' CODEC(ZSTD(1)),
    `transaction_index` UInt64 COMMENT 'The transaction index' CODEC(DoubleDelta, ZSTD(1)),
    `is_same_tx` Bool COMMENT 'Whether the self-destruct is in the same transaction as the creation' CODEC(ZSTD(1))
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}'
) PARTITION BY cityHash64(`address`) % 16
ORDER BY
    (address, block_number, transaction_hash) COMMENT 'Table for accounts self-destructs data post-6780 (Dencun fork)';

CREATE TABLE `${NETWORK_NAME}`.int_post_6780_accounts_destructs ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_post_6780_accounts_destructs_local ENGINE = Distributed(
    '{cluster}',
    `${NETWORK_NAME}`,
    int_post_6780_accounts_destructs_local,
    cityHash64(`address`)
);

CREATE TABLE `${NETWORK_NAME}`.int_accounts_alive_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the latest status of this address' CODEC(ZSTD(1)),
    `is_alive` Bool COMMENT 'Whether the account is currently alive in the state' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `block_number`
) PARTITION BY cityHash64(`address`) % 16
ORDER BY (address) COMMENT 'Table that states if an account is currently alive or not';

CREATE TABLE `${NETWORK_NAME}`.int_accounts_alive ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_accounts_alive_local ENGINE = Distributed(
    '{cluster}',
    `${NETWORK_NAME}`,
    int_accounts_alive_local,
    cityHash64(`address`)
);

CREATE TABLE `${NETWORK_NAME}`.int_address_slots_stat_per_block_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(ZSTD(1)),
    `slots_cleared` UInt16 COMMENT 'The number of slots cleared' CODEC(ZSTD(1)),
    `slots_set` UInt16 COMMENT 'The number of slots set' CODEC(ZSTD(1)),
    `net_slots` Int32 DEFAULT slots_set - slots_cleared COMMENT 'The net number of slots' CODEC(ZSTD(1)),
    `net_slots_bytes` Int32 DEFAULT (slots_set - slots_cleared) * 64 COMMENT 'The net number of raw slot bytes (slot key + value)' CODEC(ZSTD(1)),
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}'
) PARTITION BY cityHash64(`address`) % 16
ORDER BY (address, block_number) COMMENT 'Table that states the stats of the slots for an account per block';

CREATE TABLE `${NETWORK_NAME}`.int_address_slots_stat_per_block ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_slots_stat_per_block_local ENGINE = Distributed(
    '{cluster}',
    `${NETWORK_NAME}`,
    int_address_slots_stat_per_block_local,
    cityHash64(`address`)
);

CREATE TABLE `${NETWORK_NAME}`.int_block_slots_stat_local on cluster '{cluster}' (
    `block_number` UInt32 COMMENT 'The block number' CODEC(ZSTD(1)),
    `slots_cleared` UInt16 COMMENT 'The number of slots cleared' CODEC(ZSTD(1)),
    `slots_set` UInt16 COMMENT 'The number of slots set' CODEC(ZSTD(1)),
    `net_slots` Int32 DEFAULT slots_set - slots_cleared COMMENT 'The net number of slots' CODEC(ZSTD(1)),
    `net_slots_bytes` Int32 DEFAULT (slots_set - slots_cleared) * 64 COMMENT 'The net number of raw slot bytes (slot key + value)' CODEC(ZSTD(1)),
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}'
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number) COMMENT 'Table that states the stats of the slots per block';

CREATE TABLE `${NETWORK_NAME}`.int_block_slots_stat ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_block_slots_stat_local ENGINE = Distributed(
    '{cluster}',
    `${NETWORK_NAME}`,
    int_block_slots_stat_local,
    rand()
);