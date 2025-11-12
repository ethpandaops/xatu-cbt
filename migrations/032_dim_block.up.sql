CREATE TABLE `${NETWORK_NAME}`.dim_block_local ON CLUSTER '{cluster}' (
    `block_number` UInt32 COMMENT 'The execution block number' CODEC(DoubleDelta, ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'The hash of the execution block' CODEC(ZSTD(1)),
    `block_date_time` DateTime COMMENT 'The execution block date and time' CODEC(DoubleDelta, ZSTD(1)),
    `slot` Nullable(UInt32) COMMENT 'The slot number of the beacon block' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` Nullable(DateTime) COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` Nullable(UInt32) COMMENT 'The epoch number of the beacon block' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` Nullable(DateTime) COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_version` Nullable(String) COMMENT 'The version of the beacon block' CODEC(ZSTD(1)),
    `block_root` Nullable(FixedString(66)) COMMENT 'The root hash of the beacon block' CODEC(ZSTD(1)),
    `parent_root` Nullable(FixedString(66)) COMMENT 'The root hash of the parent beacon block' CODEC(ZSTD(1)),
    `state_root` Nullable(FixedString(66)) COMMENT 'The root hash of the beacon state at this block' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `block_number`
) PARTITION BY toYYYYMM(block_date_time)
ORDER BY (block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Block details for the finalized chain, this includes before and after the merge.';

CREATE TABLE `${NETWORK_NAME}`.dim_block ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.dim_block_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_block_local,
    cityHash64(`block_number`)
);
