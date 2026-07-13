CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_address_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address (lowercase hex)' CODEC(ZSTD(1)),
    `last_block_number` UInt32 COMMENT 'The last block number where this address had storage changes' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Current count of active storage slots for this address' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Current sum of effective bytes for this address' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY cityHash64(`address`) % 16 
ORDER BY (address)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Current storage slot state per address - single row per contract with latest cumulative values';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_address ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_storage_slot_state_by_address_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_by_address_local,
    cityHash64(address)
);
