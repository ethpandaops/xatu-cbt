CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `effective_bytes_from` UInt8 COMMENT 'Number of effective bytes in the first from_value of the block (0-32)' CODEC(ZSTD(1)),
    `effective_bytes_to` UInt8 COMMENT 'Number of effective bytes in the last to_value of the block (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key)
COMMENT 'Storage slot diffs aggregated per block - stores effective bytes from first and last value per address/slot';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_diff_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_diff_local,
    cityHash64(block_number, address)
);

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_by_address_slot_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `effective_bytes_from` UInt8 COMMENT 'Number of effective bytes in the first from_value of the block (0-32)' CODEC(ZSTD(1)),
    `effective_bytes_to` UInt8 COMMENT 'Number of effective bytes in the last to_value of the block (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (address, slot_key, block_number)
COMMENT 'Storage slot diffs aggregated per block - stores effective bytes from first and last value per address/slot';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_by_address_slot ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_diff_by_address_slot_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_diff_by_address_slot_local,
    cityHash64(address, slot_key)
);
