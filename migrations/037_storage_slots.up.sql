-- int_storage_slot_diff
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `effective_bytes_from` UInt8 COMMENT 'Number of effective bytes in the first from_value of the block (0-32)' CODEC(ZSTD(1)),
    `effective_bytes_to` UInt8 COMMENT 'Number of effective bytes in the last to_value of the block (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key)
COMMENT 'Storage slot diffs aggregated per block - stores effective bytes from first and last value per address/slot';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_diff_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_diff_local,
    cityHash64(block_number, address)
);

-- int_storage_slot_diff_by_address_slot
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_by_address_slot_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `effective_bytes_from` UInt8 COMMENT 'Number of effective bytes in the first from_value of the block (0-32)' CODEC(ZSTD(1)),
    `effective_bytes_to` UInt8 COMMENT 'Number of effective bytes in the last to_value of the block (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (address, slot_key, block_number)
COMMENT 'Storage slot diffs aggregated per block - stores effective bytes from first and last value per address/slot';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_by_address_slot ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_diff_by_address_slot_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_diff_by_address_slot_local,
    cityHash64(address, slot_key)
);

-- fct_storage_slot_state
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `slots_delta` Int32 COMMENT 'Change in active slots for this block (positive=activated, negative=deactivated)' CODEC(DoubleDelta, ZSTD(1)),
    `bytes_delta` Int64 COMMENT 'Change in effective bytes for this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at this block' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes across all active slots at this block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number)
COMMENT 'Cumulative storage slot state per block - tracks active slots and effective bytes with per-block deltas';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_storage_slot_state_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_local,
    cityHash64(block_number)
);

-- int_storage_slot_expiry_by_6m
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_by_6m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot expiry is recorded (6 months after it was set)' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes that were set and are now being marked for expiry (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)  PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key)
COMMENT 'Storage slot expiries - records slots that were set 6 months ago and are now candidates for clearing';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_by_6m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_expiry_by_6m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_expiry_by_6m_local,
    cityHash64(block_number, address)
);

-- fct_storage_slot_state_with_expiry_by_6m
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `net_slots_delta` Int32 COMMENT 'Net slot adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `net_bytes_delta` Int64 COMMENT 'Net bytes adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_slots` Int64 COMMENT 'Cumulative net slot adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_bytes` Int64 COMMENT 'Cumulative net bytes adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at this block (with 6-month expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes across all active slots at this block (with 6-month expiry applied)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number)
COMMENT 'Cumulative storage slot state per block with 6-month expiry policy applied - slots unused for 6 months are cleared';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_6m_local,
    cityHash64(block_number)
);

-- int_storage_slot_next_touch
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_next_touch_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot was touched' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `next_touch_block` Nullable(UInt32) COMMENT 'The next block number where this slot was touched (NULL if no subsequent touch)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key)
COMMENT 'Storage slot touches with precomputed next touch block - ordered by block_number for efficient range queries';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_next_touch ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_next_touch_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_next_touch_local,
    cityHash64(block_number, address)
);

-- int_storage_slot_read
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_read_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)  PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key)
COMMENT 'Storage slot reads aggregated per block - tracks which slots were read per address';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_read ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_read_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_read_local,
    cityHash64(block_number, address)
);

-- int_storage_slot_reactivation_by_6m
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_by_6m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot was reactivated/cancelled (touched after 6+ months of inactivity)' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes being reactivated (must match corresponding expiry record)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key)
COMMENT 'Storage slot reactivations/cancellations - records slots that were touched after 6+ months of inactivity, undoing their expiry';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_by_6m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_reactivation_by_6m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_reactivation_by_6m_local,
    cityHash64(block_number, address)
);

-- int_storage_slot_latest_state
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_latest_state_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the latest touch for this slot' CODEC(DoubleDelta, ZSTD(1)),
    `next_touch_block` Nullable(UInt32) COMMENT 'The next block where this slot was touched (NULL if no subsequent touch yet)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) ORDER BY (address, slot_key)
COMMENT 'Latest state per storage slot for efficient lookups. Helper table for int_storage_slot_next_touch.';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_latest_state ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_latest_state_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_latest_state_local,
    cityHash64(address, slot_key)
);

-- int_storage_slot_diff_latest_state
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_latest_state_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the latest diff for this slot' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes_to` UInt8 COMMENT 'Effective bytes in the final value (0 = cleared, 1-32 = active)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) ORDER BY (address, slot_key)
COMMENT 'Latest diff state per storage slot for efficient lookups. Helper table for int_storage_slot_expiry_by_6m.';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_diff_latest_state ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_diff_latest_state_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_diff_latest_state_local,
    cityHash64(address, slot_key)
);
