CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_latest_state_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the latest diff for this slot' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes_to` UInt8 COMMENT 'Effective bytes in the final value (0 = cleared, 1-32 = active)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) ORDER BY (address, slot_key)
COMMENT 'Latest diff state per storage slot for efficient lookups. Helper table for int_storage_slot_expiry_by_6m.';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_latest_state ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_diff_latest_state_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_diff_latest_state_local,
    cityHash64(address, slot_key)
);
