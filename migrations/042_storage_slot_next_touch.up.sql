CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_next_touch_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot was touched' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `next_touch_block` Nullable(UInt32) COMMENT 'The next block number where this slot was touched (NULL if no subsequent touch)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (address, slot_key, block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Storage slot touches with precomputed next touch block for efficient expiry range checks';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_next_touch ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_next_touch_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_next_touch_local,
    cityHash64(address, slot_key)
);
