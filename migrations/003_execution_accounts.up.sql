CREATE TABLE `${NETWORK_NAME}`.int_address_last_access_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the last access' CODEC(ZSTD(1)),
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `block_number`
) PARTITION BY cityHash64(`address`) % 16
ORDER BY
    (address)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Table for accounts last access data';

CREATE TABLE `${NETWORK_NAME}`.int_address_last_access ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_last_access_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_address_last_access_local,
    cityHash64(`address`)
);

CREATE TABLE `${NETWORK_NAME}`.int_address_first_access_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the first access' CODEC(ZSTD(1)),
    `version` UInt32 DEFAULT 4294967295 - block_number COMMENT 'Version for this address, for internal use in clickhouse to keep first access' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `version`
) PARTITION BY cityHash64(`address`) % 16
ORDER BY
    (address)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Table for accounts first access data';

CREATE TABLE `${NETWORK_NAME}`.int_address_first_access ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_first_access_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_address_first_access_local,
    cityHash64(`address`)
);

CREATE TABLE `${NETWORK_NAME}`.int_address_storage_slot_last_access_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The slot key of the storage' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the last access' CODEC(ZSTD(1)),
    `value` String COMMENT 'The value of the storage' CODEC(ZSTD(1)),
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `block_number`
) PARTITION BY cityHash64(`address`) % 16
ORDER BY (address, slot_key)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Table for storage last access data';

CREATE TABLE `${NETWORK_NAME}`.int_address_storage_slot_last_access ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_storage_slot_last_access_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_address_storage_slot_last_access_local,
    cityHash64(`address`, `slot_key`)
);

CREATE TABLE `${NETWORK_NAME}`.int_address_storage_slot_first_access_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The slot key of the storage' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the first access' CODEC(ZSTD(1)),
    `value` String COMMENT 'The value of the storage' CODEC(ZSTD(1)),
    `version` UInt32 DEFAULT 4294967295 - block_number COMMENT 'Version for this address + slot key, for internal use in clickhouse to keep first access' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `version`
) PARTITION BY cityHash64(`address`) % 16
ORDER BY (address, slot_key)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Table for storage first access data';

CREATE TABLE `${NETWORK_NAME}`.int_address_storage_slot_first_access ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_storage_slot_first_access_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_address_storage_slot_first_access_local,
    cityHash64(`address`, `slot_key`)
);
