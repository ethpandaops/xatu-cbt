CREATE TABLE `${NETWORK_NAME}`.int_address_diffs_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the diffs' CODEC(ZSTD(1)),
    `tx_count` UInt32 COMMENT 'The number of transactions with diffs' CODEC(ZSTD(1)),
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
    `tx_count` UInt32 COMMENT 'The number of transactions with reads' CODEC(ZSTD(1))
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