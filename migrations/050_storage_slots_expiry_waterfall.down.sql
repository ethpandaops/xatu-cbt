-- ============================================================================
-- Rollback: Restore single 6m expiry policy tables
-- ============================================================================

-- Drop unified/waterfall tables (order: distributed first, then local)
-- Hourly/daily aggregations
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_hourly_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_daily_local ON CLUSTER '{cluster}';
-- Main unified table
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_local ON CLUSTER '{cluster}';
-- Reactivation tables (all tiers)
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_24m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_24m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_18m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_18m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_12m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_12m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_6m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_1m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_1m_local ON CLUSTER '{cluster}';
-- Expiry tables (all tiers)
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_24m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_24m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_18m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_18m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_12m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_12m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_6m_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_1m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_1m_local ON CLUSTER '{cluster}';

-- ============================================================================
-- Recreate original _by_6m tables (from migration 037)
-- ============================================================================

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

-- ============================================================================
-- Recreate original _by_6m hourly/daily aggregation tables (from migration 039_storage_slot_state_agg)
-- ============================================================================

-- Hourly storage slot state with expiry
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour (with 6m expiry policy)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour (with 6m expiry policy)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`)
COMMENT 'Storage slot state metrics with 6-month expiry policy aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_6m_hourly_local,
    cityHash64(`hour_start_date_time`)
);

-- Daily storage slot state with expiry
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day (with 6m expiry policy)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day (with 6m expiry policy)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (`day_start_date`)
COMMENT 'Storage slot state metrics with 6-month expiry policy aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_6m_daily_local,
    cityHash64(`day_start_date`)
);
