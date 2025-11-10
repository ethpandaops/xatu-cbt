CREATE TABLE `${NETWORK_NAME}`.dim_block_local ON CLUSTER '{cluster}' (
    `block_number` UInt32 COMMENT 'The execution block number' CODEC(DoubleDelta, ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'The hash of the execution block' CODEC(ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` FixedString(66) COMMENT 'The root hash of the block' CODEC(ZSTD(1)),
    `parent_root` FixedString(66) COMMENT 'The root hash of the parent block' CODEC(ZSTD(1)),
    `state_root` FixedString(66) COMMENT 'The root hash of the state at the block' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `block_number`
) PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Block details for the finalized chain';

CREATE TABLE `${NETWORK_NAME}`.dim_block ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.dim_block_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_block_local,
    cityHash64(`block_number`)
);

CREATE TABLE `${NETWORK_NAME}`.int_address_activity_log_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The execution block number of the activity' CODEC(ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'Timestamp of the block' CODEC(ZSTD(1)),
    `seconds_since_last_activity` UInt32 COMMENT 'Seconds since previous activity for this address, 0 if first' CODEC(ZSTD(1)),
    `is_new_lifespan` UInt8 COMMENT '1 if this activity starts a new lifespan (gap > 6 months or first activity)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `block_number`
) PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (address, block_number)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Activity log for addresses with pre-computed gap detection for lifespan tracking';

CREATE TABLE `${NETWORK_NAME}`.int_address_activity_log ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_activity_log_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_address_activity_log_local,
    cityHash64(`address`)
);

CREATE TABLE `${NETWORK_NAME}`.int_address_lifespan_local on cluster '{cluster}' (
    `address` String COMMENT 'The address of the account' CODEC(ZSTD(1)),
    `lifespan_id` UInt32 COMMENT 'Sequential ID for each lifespan of this address (1-indexed)' CODEC(ZSTD(1)),
    `start_block_number` UInt32 COMMENT 'First block number of this lifespan' CODEC(ZSTD(1)),
    `end_block_number` UInt32 COMMENT 'Last block number of this lifespan' CODEC(ZSTD(1)),
    `start_timestamp` DateTime COMMENT 'Timestamp of first activity in this lifespan' CODEC(ZSTD(1)),
    `end_timestamp` DateTime COMMENT 'Timestamp of last activity in this lifespan' CODEC(ZSTD(1)),
    `last_activity_block` UInt32 COMMENT 'Most recent activity block (used as version for ReplacingMergeTree)' CODEC(ZSTD(1)),
    `is_active` UInt8 COMMENT 'Whether this lifespan is currently active (1) or ended due to 6-month gap (0)' CODEC(ZSTD(1)),
    `activity_count` UInt32 COMMENT 'Number of distinct activities observed in this lifespan' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `last_activity_block`
) PARTITION BY cityHash64(`address`) % 16
ORDER BY (address, lifespan_id)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Account lifespans - periods of activity separated by gaps > 6 months';

CREATE TABLE `${NETWORK_NAME}`.int_address_lifespan ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_address_lifespan_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_address_lifespan_local,
    cityHash64(`address`)
);
