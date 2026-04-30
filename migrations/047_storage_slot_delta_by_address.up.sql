CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_delta_by_address_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address (lowercase hex)' CODEC(ZSTD(1)),
    `slots_delta` Int32 COMMENT 'Change in active slots for this address in this block (positive=activated, negative=deactivated)' CODEC(DoubleDelta, ZSTD(1)),
    `bytes_delta` Int64 COMMENT 'Change in effective bytes for this address in this block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (address, block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Storage slot deltas per block per address - tracks slot and byte changes for each contract';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_delta_by_address ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_storage_slot_delta_by_address_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_delta_by_address_local,
    cityHash64(address)
);
