CREATE TABLE `${NETWORK_NAME}`.int_execution_block_by_date_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_date_time` DateTime64(3) COMMENT 'The block timestamp' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(block_date_time)
ORDER BY (block_date_time, block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Execution blocks ordered by timestamp for efficient date range lookups';

CREATE TABLE `${NETWORK_NAME}`.int_execution_block_by_date ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_execution_block_by_date_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_execution_block_by_date_local,
    cityHash64(block_number)
);
