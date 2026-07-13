CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_top_10_by_effective_bytes_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `rank` UInt32 COMMENT 'Rank by effective bytes (1=highest)' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Current count of active storage slots for this address' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Total effective bytes of storage slots for this address' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`rank`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Top 10 addresses by effective bytes of storage slots';

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_top_10_by_effective_bytes ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_address_storage_slot_top_10_by_effective_bytes_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_address_storage_slot_top_10_by_effective_bytes_local,
    cityHash64(`rank`)
);
