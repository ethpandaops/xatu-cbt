-- ============================================================================
-- Migration: Waterfall Expiry Policy System for Storage Slots
-- ============================================================================
-- Replaces the single 6m expiry policy with a waterfall system:
--   1m (base) -> 6m -> 12m -> 18m -> 24m
-- Each tier waterfalls from the previous, dramatically reducing query complexity.
--
-- Key changes:
--   - expiry tables include touch_block to enable waterfall lookups
--   - per-tier reactivation tables track when expiries are undone
--   - waterfall expiry models check previous tier's reactivation
-- Each policy tier has its own expiry and reactivation table.
-- ============================================================================

-- Drop old policy-specific tables (order: distributed first, then local)
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_by_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_by_6m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_by_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_by_6m_local ON CLUSTER '{cluster}';

-- ============================================================================
-- int_storage_slot_expiry_1m (base tier)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_1m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes that were set (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 1-month expiries - base tier of waterfall chain';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_1m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_expiry_1m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_expiry_1m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_reactivation_1m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_1m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes being reactivated (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 1-month reactivations - slots touched after 1m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_1m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_reactivation_1m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_reactivation_1m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_expiry_6m (waterfalls from 1m)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_6m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes that were set (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 6-month expiries - waterfalls from 1m tier';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_6m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_expiry_6m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_expiry_6m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_reactivation_6m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_6m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes being reactivated (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 6-month reactivations - slots touched after 6m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_6m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_reactivation_6m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_reactivation_6m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_expiry_12m (waterfalls from 6m)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_12m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes that were set (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 12-month expiries - waterfalls from 6m tier';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_12m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_expiry_12m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_expiry_12m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_reactivation_12m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_12m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes being reactivated (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 12-month reactivations - slots touched after 12m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_12m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_reactivation_12m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_reactivation_12m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_expiry_18m (waterfalls from 12m)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_18m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes that were set (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 18-month expiries - waterfalls from 12m tier';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_18m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_expiry_18m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_expiry_18m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_reactivation_18m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_18m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes being reactivated (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 18-month reactivations - slots touched after 18m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_18m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_reactivation_18m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_reactivation_18m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_expiry_24m (waterfalls from 18m)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_24m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes that were set (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 24-month expiries - waterfalls from 18m tier';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_expiry_24m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_expiry_24m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_expiry_24m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_storage_slot_reactivation_24m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_24m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this slot was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slot_key` String COMMENT 'The storage slot key' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` UInt8 COMMENT 'Number of effective bytes being reactivated (0-32)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, slot_key, touch_block)
COMMENT 'Storage slot 24-month reactivations - slots touched after 24m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_reactivation_24m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_reactivation_24m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_reactivation_24m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- fct_storage_slot_state_with_expiry (unified, partitioned by policy)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `net_slots_delta` Int32 COMMENT 'Net slot adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `net_bytes_delta` Int64 COMMENT 'Net bytes adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_slots` Int64 COMMENT 'Cumulative net slot adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_bytes` Int64 COMMENT 'Cumulative net bytes adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at this block (with expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at this block (with expiry applied)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY expiry_policy
ORDER BY (expiry_policy, block_number)
COMMENT 'Cumulative storage slot state with expiry policies - supports 1m, 6m, 12m, 18m, 24m waterfall';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_local,
    cityHash64(block_number)
);

-- ============================================================================
-- Drop old 6m-specific hourly/daily aggregation tables
-- ============================================================================
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_hourly_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_daily_local ON CLUSTER '{cluster}';

-- ============================================================================
-- fct_storage_slot_state_with_expiry_hourly (unified, with expiry_policy)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour (with expiry applied)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour (with expiry applied)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, toStartOfMonth(hour_start_date_time))
ORDER BY (expiry_policy, hour_start_date_time)
COMMENT 'Storage slot state metrics with expiry policies aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_hourly_local,
    cityHash64(expiry_policy, hour_start_date_time)
);

-- ============================================================================
-- fct_storage_slot_state_with_expiry_daily (unified, with expiry_policy)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day (with expiry applied)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day (with expiry applied)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, toYYYYMM(day_start_date))
ORDER BY (expiry_policy, day_start_date)
COMMENT 'Storage slot state metrics with expiry policies aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_daily_local,
    cityHash64(expiry_policy, day_start_date)
);
