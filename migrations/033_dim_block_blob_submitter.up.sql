CREATE TABLE `${NETWORK_NAME}`.dim_block_blob_submitter_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash' CODEC(ZSTD(1)),
    `address` String COMMENT 'The blob submitter address' CODEC(ZSTD(1)),
    `name` Nullable(String) COMMENT 'The name of the blob submitter' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 500000)
ORDER BY (block_number, transaction_hash)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Blob transaction to submitter name mapping.';

CREATE TABLE `${NETWORK_NAME}`.dim_block_blob_submitter ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.dim_block_blob_submitter_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_block_blob_submitter_local,
    cityHash64(`block_number`, `transaction_hash`)
);
