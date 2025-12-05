CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_1y_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_slots_delta` Int32 COMMENT 'Slots expired this block (always <= 0)' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_bytes_delta` Int64 COMMENT 'Bytes freed by expiry this block (always <= 0)' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_expiry_slots` Int64 COMMENT 'Cumulative slots removed by expiry up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_expiry_bytes` Int64 COMMENT 'Cumulative bytes freed by expiry up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at this block (with 1-year expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes across all active slots at this block (with 1-year expiry applied)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Cumulative storage slot state per block with 1-year expiry policy applied - slots unused for 1 year are cleared';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_1y ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_1y_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_1y_local,
    cityHash64(block_number)
);
