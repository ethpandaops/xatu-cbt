CREATE TABLE `${NETWORK_NAME}`.dim_block_canonical_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The execution block number' CODEC(DoubleDelta, ZSTD(1)),
    `execution_block_hash` FixedString(66) COMMENT 'The hash of the execution block' CODEC(ZSTD(1)),
    `block_date_time` DateTime COMMENT 'The execution block date and time' CODEC(DoubleDelta, ZSTD(1)),
    `slot` Nullable(UInt32) COMMENT 'The slot number of the beacon block' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` Nullable(DateTime) COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` Nullable(UInt32) COMMENT 'The epoch number of the beacon block' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` Nullable(DateTime) COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `beacon_block_version` Nullable(String) COMMENT 'The version of the beacon block' CODEC(ZSTD(1)),
    `beacon_block_root` Nullable(FixedString(66)) COMMENT 'The root hash of the beacon block' CODEC(ZSTD(1)),
    `beacon_parent_root` Nullable(FixedString(66)) COMMENT 'The root hash of the parent beacon block' CODEC(ZSTD(1)),
    `beacon_state_root` Nullable(FixedString(66)) COMMENT 'The root hash of the beacon state at this block' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(block_date_time)
ORDER BY (block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Block details for the finalized chain, this includes before and after the merge.';

CREATE TABLE `${NETWORK_NAME}`.dim_block_canonical ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.dim_block_canonical_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_block_canonical_local,
    cityHash64(`block_number`)
);
