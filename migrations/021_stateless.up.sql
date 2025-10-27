CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_top_100_by_contract_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `rank` UInt32 COMMENT 'Rank by total storage slots (1=highest)' CODEC(DoubleDelta, ZSTD(1)),
    `contract_address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `total_storage_slots` UInt64 COMMENT 'Total number of storage slots for this contract' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`rank`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 4,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Top 100 contracts by storage slots';

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_top_100_by_contract ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_address_storage_slot_top_100_by_contract_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_address_storage_slot_top_100_by_contract_local,
    cityHash64(`rank`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_expired_top_100_by_contract_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `rank` UInt32 COMMENT 'Rank by expired storage slots (1=highest)' CODEC(DoubleDelta, ZSTD(1)),
    `contract_address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `expired_slots` UInt64 COMMENT 'Number of expired storage slots for this contract' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`rank`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 4,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Top 100 contracts by expired storage slots (not accessed in last 365 days)';

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_expired_top_100_by_contract ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_address_storage_slot_expired_top_100_by_contract_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_address_storage_slot_expired_top_100_by_contract_local,
    cityHash64(`rank`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_address_access_total_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `total_accounts` UInt64 COMMENT 'Total number of accounts accessed in last 365 days' CODEC(ZSTD(1)),
    `expired_accounts` UInt64 COMMENT 'Number of expired accounts (not accessed in last 365 days)' CODEC(ZSTD(1)),
    `total_contract_accounts` UInt64 COMMENT 'Total number of contract accounts accessed in last 365 days' CODEC(ZSTD(1)),
    `expired_contracts` UInt64 COMMENT 'Number of expired contracts (not accessed in last 365 days)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`updated_date_time`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 4,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Address access totals and expiry statistics';

CREATE TABLE `${NETWORK_NAME}`.fct_address_access_total ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_address_access_total_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_address_access_total_local,
    rand()
);

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_total_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `total_storage_slots` UInt64 COMMENT 'Total number of storage slots accessed in last 365 days' CODEC(ZSTD(1)),
    `expired_storage_slots` UInt64 COMMENT 'Number of expired storage slots (not accessed in last 365 days)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`updated_date_time`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 4,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Storage slot totals and expiry statistics';

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_total ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_address_storage_slot_total_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_address_storage_slot_total_local,
    rand()
);

CREATE TABLE `${NETWORK_NAME}`.fct_address_access_chunked_10000_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `chunk_start_block_number` UInt32 COMMENT 'Start block number of the chunk' CODEC(ZSTD(1)),
    `first_accessed_accounts` UInt32 COMMENT 'Number of accounts first accessed in the chunk' CODEC(ZSTD(1)),
    `last_accessed_accounts` UInt32 COMMENT 'Number of accounts last accessed in the chunk' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`chunk_start_block_number`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 4,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Address access totals chunked by 10000 blocks';

CREATE TABLE `${NETWORK_NAME}`.fct_address_access_chunked_10000 ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_address_access_chunked_10000_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_address_access_chunked_10000_local,
    rand()
);

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_chunked_10000_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `chunk_start_block_number` UInt32 COMMENT 'Start block number of the chunk' CODEC(ZSTD(1)),
    `first_accessed_slots` UInt32 COMMENT 'Number of slots first accessed in the chunk' CODEC(ZSTD(1)),
    `last_accessed_slots` UInt32 COMMENT 'Number of slots last accessed in the chunk' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`chunk_start_block_number`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 4,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Storage slot totals chunked by 10000 blocks';

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_chunked_10000 ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_address_storage_slot_chunked_10000_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_address_storage_slot_chunked_10000_local,
    rand()
);
