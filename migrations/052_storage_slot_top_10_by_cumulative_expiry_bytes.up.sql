CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_top_10_by_cumulative_expiry_bytes_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `rank` UInt32 COMMENT 'Rank by cumulative expiry bytes (1=highest)' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `cumulative_expiry_slots` Int64 COMMENT 'Cumulative slots removed by 1-year expiry for this address' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_expiry_bytes` Int64 COMMENT 'Cumulative bytes freed by 1-year expiry for this address' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Current count of active storage slots for this address (with 1-year expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Current sum of effective bytes for this address (with 1-year expiry applied)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`rank`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Top 10 addresses by cumulative expiry bytes (1-year expiry policy)';

CREATE TABLE `${NETWORK_NAME}`.fct_address_storage_slot_top_10_by_cumulative_expiry_bytes ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_address_storage_slot_top_10_by_cumulative_expiry_bytes_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_address_storage_slot_top_10_by_cumulative_expiry_bytes_local,
    cityHash64(`rank`)
);
