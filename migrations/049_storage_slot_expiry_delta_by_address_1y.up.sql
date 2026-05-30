CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_expiry_delta_by_address_1y_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where expiries occurred' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address (lowercase hex)' CODEC(ZSTD(1)),
    `expiry_slots_delta` Int32 COMMENT 'Slots expired this block for this address (always <= 0)' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_bytes_delta` Int64 COMMENT 'Bytes freed by expiry this block for this address (always <= 0)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (address, block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Storage slot expiry deltas per block per address with 1-year expiry policy - tracks slots cleared due to inactivity';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_expiry_delta_by_address_1y ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_storage_slot_expiry_delta_by_address_1y_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_expiry_delta_by_address_1y_local,
    cityHash64(address)
);
