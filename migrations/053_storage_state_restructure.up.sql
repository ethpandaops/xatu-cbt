-- ============================================================================
-- Migration 053: Storage Slot State Restructuring + Top 100 + Contract-Level
-- ============================================================================
-- This migration:
--   1. Drops old fct_storage_slot_state* tables (replaced by int_ and _by_block variants)
--   2. Creates new int_storage_slot_state* tables (per block, per address)
--   3. Creates new fct_storage_slot_state_by_* hourly/daily aggregations
--   4. Creates top 100 contracts tables
--   5. Creates contract-level storage tracking tables
-- ============================================================================

-- ============================================================================
-- DROP OLD TABLES (from migrations 037, 039, 050)
-- ============================================================================

-- From migration 037: fct_storage_slot_state (renamed to int_storage_slot_state_by_block)
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_local ON CLUSTER '{cluster}';

-- From migration 039: fct_storage_slot_state_hourly/daily (renamed to fct_storage_slot_state_by_block_*)
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_hourly_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_daily_local ON CLUSTER '{cluster}';

-- From migration 050: fct_storage_slot_state_with_expiry* (renamed to int_storage_slot_state_with_expiry_by_block)
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_hourly_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_daily_local ON CLUSTER '{cluster}';

-- ============================================================================
-- NEW SLOT-LEVEL STATE TABLES (replacing fct_storage_slot_state from 037)
-- ============================================================================

-- int_storage_slot_state (per block, address)
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slots_delta` Int32 COMMENT 'Change in active slots for this block (positive=activated, negative=deactivated)' CODEC(DoubleDelta, ZSTD(1)),
    `bytes_delta` Int64 COMMENT 'Change in effective bytes for this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots for this address at this block' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes for this address at this block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address)
COMMENT 'Cumulative storage slot state per block per address - tracks active slots and effective bytes with per-block deltas';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_state_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_state_local,
    cityHash64(block_number, address)
);

-- int_storage_slot_state_by_address (same data as int_storage_slot_state, ordered by address for efficient address lookups)
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_by_address_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slots_delta` Int32 COMMENT 'Change in active slots for this block (positive=activated, negative=deactivated)' CODEC(DoubleDelta, ZSTD(1)),
    `bytes_delta` Int64 COMMENT 'Change in effective bytes for this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots for this address at this block' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes for this address at this block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (address, block_number)
COMMENT 'Cumulative storage slot state per block per address - ordered by address for efficient address-based queries';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_by_address ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_state_by_address_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_state_by_address_local,
    cityHash64(address, block_number)
);

-- int_storage_slot_state_by_block (aggregated per block from int_storage_slot_state)
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_by_block_local ON CLUSTER '{cluster}' (
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

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_by_block ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_state_by_block_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_state_by_block_local,
    cityHash64(block_number)
);

-- ============================================================================
-- NEW SLOT-LEVEL HOURLY/DAILY AGGREGATIONS (replacing from 039)
-- ============================================================================

-- Hourly storage slot state by block (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_block_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`)
COMMENT 'Storage slot state metrics aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_block_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_by_block_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_by_block_hourly_local,
    cityHash64(`hour_start_date_time`)
);

-- Daily storage slot state by block (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_block_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (`day_start_date`)
COMMENT 'Storage slot state metrics aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_block_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_by_block_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_by_block_daily_local,
    cityHash64(`day_start_date`)
);

-- Hourly storage slot state by address (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_address_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (address, `hour_start_date_time`)
COMMENT 'Storage slot state metrics per address aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_address_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_by_address_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_by_address_hourly_local,
    cityHash64(address, `hour_start_date_time`)
);

-- Daily storage slot state by address (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_address_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (address, `day_start_date`)
COMMENT 'Storage slot state metrics per address aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_by_address_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_by_address_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_by_address_daily_local,
    cityHash64(address, `day_start_date`)
);

-- ============================================================================
-- NEW SLOT-LEVEL STATE WITH EXPIRY TABLES (replacing from 050)
-- ============================================================================

-- int_storage_slot_state_with_expiry (per block, address, expiry_policy)
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `net_slots_delta` Int32 COMMENT 'Net slot adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `net_bytes_delta` Int64 COMMENT 'Net bytes adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_slots` Int64 COMMENT 'Cumulative net slot adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_bytes` Int64 COMMENT 'Cumulative net bytes adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots for this address at this block (with expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes for this address at this block (with expiry applied)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, intDiv(block_number, 5000000))
ORDER BY (expiry_policy, block_number, address)
COMMENT 'Cumulative storage slot state per block per address with expiry policies - supports 1m, 6m, 12m, 18m, 24m waterfall';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_with_expiry ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_state_with_expiry_local,
    cityHash64(block_number, address)
);

-- int_storage_slot_state_with_expiry_by_address (same data, ordered by address for efficient address lookups)
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_address_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `net_slots_delta` Int32 COMMENT 'Net slot adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `net_bytes_delta` Int64 COMMENT 'Net bytes adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_slots` Int64 COMMENT 'Cumulative net slot adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_bytes` Int64 COMMENT 'Cumulative net bytes adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots for this address at this block (with expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes for this address at this block (with expiry applied)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, intDiv(block_number, 5000000))
ORDER BY (address, expiry_policy, block_number)
COMMENT 'Cumulative storage slot state per block per address with expiry policies - ordered by address for efficient address lookups';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_address ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_address_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_state_with_expiry_by_address_local,
    cityHash64(address, block_number)
);

-- int_storage_slot_state_with_expiry_by_block (aggregated from int_storage_slot_state_with_expiry)
CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_block_local ON CLUSTER '{cluster}' (
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
COMMENT 'Cumulative storage slot state per block with expiry policies - supports 1m, 6m, 12m, 18m, 24m waterfall';

CREATE TABLE `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_block ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_block_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_storage_slot_state_with_expiry_by_block_local,
    cityHash64(block_number)
);

-- fct_storage_slot_state_with_expiry_by_block_hourly (unified, with expiry_policy)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_hourly_local ON CLUSTER '{cluster}' (
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

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_block_hourly_local,
    cityHash64(expiry_policy, hour_start_date_time)
);

-- fct_storage_slot_state_with_expiry_by_block_daily (unified, with expiry_policy)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_daily_local ON CLUSTER '{cluster}' (
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

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_block_daily_local,
    cityHash64(expiry_policy, day_start_date)
);

-- fct_storage_slot_state_with_expiry_by_address_hourly (with expiry_policy and address)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour (with expiry applied)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour (with expiry applied)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, toStartOfMonth(hour_start_date_time))
ORDER BY (address, expiry_policy, hour_start_date_time)
COMMENT 'Storage slot state metrics per address with expiry policies aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_address_hourly_local,
    cityHash64(address, expiry_policy, hour_start_date_time)
);

-- fct_storage_slot_state_with_expiry_by_address_daily (with expiry_policy and address)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day (with expiry applied)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day (with expiry applied)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, toYYYYMM(day_start_date))
ORDER BY (address, expiry_policy, day_start_date)
COMMENT 'Storage slot state metrics per address with expiry policies aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_with_expiry_by_address_daily_local,
    cityHash64(address, expiry_policy, day_start_date)
);

-- ============================================================================
-- TOP 100 CONTRACTS TABLES
-- ============================================================================

-- ============================================================================
-- fct_storage_slot_top_100_by_slots
-- Top 100 contracts by active storage slot count with expiry policies
-- Rank is based on raw state (NULL expiry_policy), with expiry values shown
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_top_100_by_slots_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` Nullable(String) COMMENT 'Expiry policy identifier: NULL (raw), 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `rank` UInt32 COMMENT 'Rank by active slots (1=highest), based on raw state' CODEC(DoubleDelta, ZSTD(1)),
    `contract_address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Number of active storage slots for this contract' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Effective bytes of storage for this contract' CODEC(ZSTD(1)),
    `owner_key` Nullable(String) COMMENT 'Owner key identifier' CODEC(ZSTD(1)),
    `account_owner` Nullable(String) COMMENT 'Account owner of the contract' CODEC(ZSTD(1)),
    `contract_name` Nullable(String) COMMENT 'Name of the contract' CODEC(ZSTD(1)),
    `factory_contract` Nullable(String) COMMENT 'Factory contract or deployer address' CODEC(ZSTD(1)),
    `labels` Array(String) COMMENT 'Labels/categories (e.g., stablecoin, dex, circle)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`rank`, ifNull(`expiry_policy`, ''))
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Top 100 contracts by active storage slot count with expiry policies applied';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_top_100_by_slots ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_top_100_by_slots_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_top_100_by_slots_local,
    cityHash64(`rank`)
);

-- ============================================================================
-- fct_storage_slot_top_100_by_bytes
-- Top 100 contracts by effective bytes with expiry policies
-- Rank is based on raw state (NULL expiry_policy), with expiry values shown
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_top_100_by_bytes_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` Nullable(String) COMMENT 'Expiry policy identifier: NULL (raw), 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `rank` UInt32 COMMENT 'Rank by effective bytes (1=highest), based on raw state' CODEC(DoubleDelta, ZSTD(1)),
    `contract_address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Effective bytes of storage for this contract' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Number of active storage slots for this contract' CODEC(ZSTD(1)),
    `owner_key` Nullable(String) COMMENT 'Owner key identifier' CODEC(ZSTD(1)),
    `account_owner` Nullable(String) COMMENT 'Account owner of the contract' CODEC(ZSTD(1)),
    `contract_name` Nullable(String) COMMENT 'Name of the contract' CODEC(ZSTD(1)),
    `factory_contract` Nullable(String) COMMENT 'Factory contract or deployer address' CODEC(ZSTD(1)),
    `labels` Array(String) COMMENT 'Labels/categories (e.g., stablecoin, dex, circle)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`rank`, ifNull(`expiry_policy`, ''))
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Top 100 contracts by effective storage bytes with expiry policies applied';

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_top_100_by_bytes ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_top_100_by_bytes_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_top_100_by_bytes_local,
    cityHash64(`rank`)
);

-- ============================================================================
-- Migration: Dimension Table for Contract Owner Information
-- ============================================================================
-- Creates a dimension table that stores contract ownership metadata from
-- Dune Analytics and growthepie for contracts that appear in the top 100
-- storage slot tables.
-- ============================================================================

CREATE TABLE `${NETWORK_NAME}`.dim_contract_owner_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `contract_address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `owner_key` Nullable(String) COMMENT 'Owner key identifier' CODEC(ZSTD(1)),
    `account_owner` Nullable(String) COMMENT 'Account owner of the contract' CODEC(ZSTD(1)),
    `contract_name` Nullable(String) COMMENT 'Name of the contract' CODEC(ZSTD(1)),
    `factory_contract` Nullable(String) COMMENT 'Factory contract or deployer address' CODEC(ZSTD(1)),
    `labels` Array(String) COMMENT 'Labels/categories (e.g., stablecoin, dex, circle)' CODEC(ZSTD(1)),
    `sources` Array(String) COMMENT 'Sources of the label data (e.g., growthepie, dune, eth-labels)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY tuple()
ORDER BY (`contract_address`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Contract owner information from Dune Analytics, growthepie, and eth-labels for top storage slot contracts';

CREATE TABLE `${NETWORK_NAME}`.dim_contract_owner ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.dim_contract_owner_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_contract_owner_local,
    cityHash64(`contract_address`)
);

-- ============================================================================
-- Migration: Contract-Level Storage Expiry System
-- ============================================================================
-- Parallel to the slot-level expiry system, but tracks at contract (address) level.
-- A contract "expires" when NO slot in that contract has been touched for the
-- given expiry period (1m, 6m, 12m, 18m, 24m).
--
-- Key differences from slot-level:
--   - Groups by (address) only, not (address, slot_key)
--   - effective_bytes is UInt64 (sum of all slots) not UInt8 (single slot)
--   - A "touch" = any slot read or write on the contract
-- ============================================================================

-- ============================================================================
-- int_contract_storage_next_touch
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_next_touch_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract was touched' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `next_touch_block` Nullable(UInt32) COMMENT 'The next block number where this contract was touched (NULL if no subsequent touch)' CODEC(ZSTD(1)),
    -- Projection for efficient next_touch_block filtering without FINAL
    PROJECTION proj_by_next_touch_block
    (
        SELECT *
        ORDER BY (next_touch_block, block_number, address)
    )
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Contract-level touches with precomputed next touch block - a touch is any slot read or write on the contract';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_next_touch ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_next_touch_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_next_touch_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- helper_contract_storage_next_touch_latest_state
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.helper_contract_storage_next_touch_latest_state_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number of the latest touch for this contract' CODEC(DoubleDelta, ZSTD(1)),
    `next_touch_block` Nullable(UInt32) COMMENT 'The next block where this contract was touched (NULL if no subsequent touch yet)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) ORDER BY (address)
COMMENT 'Latest state per contract for efficient lookups. Helper table for int_contract_storage_next_touch.';

CREATE TABLE `${NETWORK_NAME}`.helper_contract_storage_next_touch_latest_state ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.helper_contract_storage_next_touch_latest_state_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    helper_contract_storage_next_touch_latest_state_local,
    cityHash64(address)
);

-- ============================================================================
-- int_contract_storage_expiry_1m (base tier)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_1m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots in the contract at expiry time' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes across all slots in the contract at expiry time' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 1-month expiries - base tier of waterfall chain';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_1m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_expiry_1m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_expiry_1m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_reactivation_1m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_1m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots being reactivated' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes being reactivated' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 1-month reactivations - contracts touched after 1m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_1m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_reactivation_1m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_reactivation_1m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_expiry_6m (waterfalls from 1m)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_6m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots in the contract at expiry time' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes across all slots in the contract at expiry time' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 6-month expiries - waterfalls from 1m tier';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_6m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_expiry_6m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_expiry_6m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_reactivation_6m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_6m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots being reactivated' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes being reactivated' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 6-month reactivations - contracts touched after 6m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_6m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_reactivation_6m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_reactivation_6m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_expiry_12m (waterfalls from 6m)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_12m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots in the contract at expiry time' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes across all slots in the contract at expiry time' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 12-month expiries - waterfalls from 6m tier';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_12m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_expiry_12m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_expiry_12m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_reactivation_12m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_12m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots being reactivated' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes being reactivated' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 12-month reactivations - contracts touched after 12m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_12m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_reactivation_12m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_reactivation_12m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_expiry_18m (waterfalls from 12m)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_18m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots in the contract at expiry time' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes across all slots in the contract at expiry time' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 18-month expiries - waterfalls from 12m tier';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_18m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_expiry_18m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_expiry_18m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_reactivation_18m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_18m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots being reactivated' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes being reactivated' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 18-month reactivations - contracts touched after 18m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_18m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_reactivation_18m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_reactivation_18m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_expiry_24m (waterfalls from 18m)
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_24m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract expiry is recorded' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that led to this expiry (propagates through waterfall chain)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots in the contract at expiry time' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes across all slots in the contract at expiry time' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 24-month expiries - waterfalls from 18m tier';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_expiry_24m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_expiry_24m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_expiry_24m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_reactivation_24m
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_24m_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number where this contract was reactivated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `touch_block` UInt32 COMMENT 'The original touch block that expired (for matching with expiry records)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` UInt64 COMMENT 'Count of slots being reactivated' CODEC(ZSTD(1)),
    `effective_bytes` UInt64 COMMENT 'Sum of effective bytes being reactivated' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address, touch_block)
COMMENT 'Contract-level 24-month reactivations - contracts touched after 24m expiry';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_reactivation_24m ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_reactivation_24m_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_reactivation_24m_local,
    cityHash64(block_number, address)
);

-- ============================================================================
-- int_contract_storage_state_with_expiry_by_address (per address state)
-- ============================================================================
-- Tracks per-contract storage state under each expiry policy.
-- active_slots and effective_bytes reflect the contract state after expiry adjustments.
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_address_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Number of active storage slots in this contract (0 if expired)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Effective bytes for this contract (0 if expired)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, intDiv(block_number, 5000000))
ORDER BY (address, expiry_policy, block_number)
COMMENT 'Contract-level expiry state ordered by address - tracks active_slots and effective_bytes per contract';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_address ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_address_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_state_with_expiry_by_address_local,
    cityHash64(address, block_number)
);

-- ============================================================================
-- int_contract_storage_state_with_expiry_by_block (network-wide aggregation)
-- ============================================================================
-- Network-wide totals: sum of slots, sum of bytes, count of active contracts.
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_block_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Total active storage slots network-wide (with expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Total effective bytes network-wide (with expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `active_contracts` Int64 COMMENT 'Count of contracts with active_slots > 0 (with expiry applied)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY expiry_policy
ORDER BY (expiry_policy, block_number)
COMMENT 'Contract-level expiry state per block network-wide - totals for slots, bytes, and active contracts';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_block ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_block_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_state_with_expiry_by_block_local,
    cityHash64(block_number)
);

-- ============================================================================
-- fct_contract_storage_state_with_expiry_by_block_hourly
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Total active storage slots at end of hour (with expiry applied)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Total effective bytes at end of hour (with expiry applied)' CODEC(ZSTD(1)),
    `active_contracts` Int64 COMMENT 'Count of contracts with active_slots > 0 at end of hour' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, toStartOfMonth(hour_start_date_time))
ORDER BY (expiry_policy, hour_start_date_time)
COMMENT 'Contract-level expiry state metrics aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_contract_storage_state_with_expiry_by_block_hourly_local,
    cityHash64(expiry_policy, hour_start_date_time)
);

-- ============================================================================
-- fct_contract_storage_state_with_expiry_by_block_daily
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Total active storage slots at end of day (with expiry applied)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Total effective bytes at end of day (with expiry applied)' CODEC(ZSTD(1)),
    `active_contracts` Int64 COMMENT 'Count of contracts with active_slots > 0 at end of day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, toYYYYMM(day_start_date))
ORDER BY (expiry_policy, day_start_date)
COMMENT 'Contract-level expiry state metrics aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_contract_storage_state_with_expiry_by_block_daily_local,
    cityHash64(expiry_policy, day_start_date)
);

-- ============================================================================
-- fct_contract_storage_state_with_expiry_by_address_hourly
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Active storage slots in this contract at end of hour (0 if expired)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Effective bytes at end of hour (0 if expired)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, toStartOfMonth(hour_start_date_time))
ORDER BY (address, expiry_policy, hour_start_date_time)
COMMENT 'Contract-level expiry state metrics per address aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_contract_storage_state_with_expiry_by_address_hourly_local,
    cityHash64(address, expiry_policy, hour_start_date_time)
);

-- ============================================================================
-- fct_contract_storage_state_with_expiry_by_address_daily
-- ============================================================================
CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `active_slots` Int64 COMMENT 'Active storage slots in this contract at end of day (0 if expired)' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Effective bytes at end of day (0 if expired)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, toYYYYMM(day_start_date))
ORDER BY (address, expiry_policy, day_start_date)
COMMENT 'Contract-level expiry state metrics per address aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_contract_storage_state_with_expiry_by_address_daily_local,
    cityHash64(address, expiry_policy, day_start_date)
);

-- ============================================================================
-- Migration: int_contract_storage_state_with_expiry base table
-- ============================================================================
-- Base table for contract-level expiry state, following the same pattern as
-- int_storage_slot_state_with_expiry for slot-level expiry.
--
-- This table tracks:
--   - net_slots_delta, net_bytes_delta: per-block adjustments from expiry/reactivation
--   - cumulative_net_slots, cumulative_net_bytes: running totals of adjustments
--   - active_slots, effective_bytes: final values (base + cumulative)
-- ============================================================================

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `expiry_policy` LowCardinality(String) COMMENT 'Expiry policy identifier: 1m, 6m, 12m, 18m, 24m' CODEC(ZSTD(1)),
    `net_slots_delta` Int32 COMMENT 'Net slot adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `net_bytes_delta` Int64 COMMENT 'Net bytes adjustment this block (negative=expiry, positive=reactivation)' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_slots` Int64 COMMENT 'Cumulative net slot adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `cumulative_net_bytes` Int64 COMMENT 'Cumulative net bytes adjustment up to this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Number of active storage slots in this contract (with expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Effective bytes for this contract (with expiry applied)' CODEC(DoubleDelta, ZSTD(1)),
    `prev_active_slots` Int64 COMMENT 'Previous block active_slots for this address (for transition detection)' CODEC(DoubleDelta, ZSTD(1)),
    `prev_effective_bytes` Int64 COMMENT 'Previous block effective_bytes for this address (for delta calculation)' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY (expiry_policy, intDiv(block_number, 5000000))
ORDER BY (expiry_policy, block_number, address)
COMMENT 'Contract-level expiry state base table - tracks deltas and cumulative adjustments per address per policy';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_with_expiry ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_state_with_expiry_local,
    cityHash64(block_number, address)
);

-- int_contract_storage_state (per block, address) - same structure as int_storage_slot_state
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slots_delta` Int32 COMMENT 'Change in active slots for this block (positive=activated, negative=deactivated)' CODEC(DoubleDelta, ZSTD(1)),
    `bytes_delta` Int64 COMMENT 'Change in effective bytes for this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots for this contract at this block' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes for this contract at this block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number, address)
COMMENT 'Cumulative contract storage state per block per address - tracks active slots and effective bytes with per-block deltas';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_state_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_state_local,
    cityHash64(block_number, address)
);

-- int_contract_storage_state_by_address (same data as int_contract_storage_state, ordered by address for efficient address lookups)
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_by_address_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `slots_delta` Int32 COMMENT 'Change in active slots for this block (positive=activated, negative=deactivated)' CODEC(DoubleDelta, ZSTD(1)),
    `bytes_delta` Int64 COMMENT 'Change in effective bytes for this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots for this contract at this block' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes for this contract at this block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (address, block_number)
COMMENT 'Cumulative contract storage state per block per address - ordered by address for efficient address-based queries';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_by_address ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_state_by_address_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_state_by_address_local,
    cityHash64(address, block_number)
);

-- int_contract_storage_state_by_block (aggregated per block with active_contracts count)
CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_by_block_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `block_number` UInt32 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),
    `slots_delta` Int32 COMMENT 'Change in active slots for this block (positive=activated, negative=deactivated)' CODEC(DoubleDelta, ZSTD(1)),
    `bytes_delta` Int64 COMMENT 'Change in effective bytes for this block' CODEC(DoubleDelta, ZSTD(1)),
    `contracts_delta` Int32 COMMENT 'Change in active contracts for this block (positive=activated, negative=deactivated)' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at this block' CODEC(DoubleDelta, ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes across all active slots at this block' CODEC(DoubleDelta, ZSTD(1)),
    `active_contracts` Int64 COMMENT 'Cumulative count of contracts with at least one active slot at this block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY intDiv(block_number, 5000000)
ORDER BY (block_number)
COMMENT 'Cumulative contract storage state per block - tracks active slots, effective bytes, and active contracts with per-block deltas';

CREATE TABLE `${NETWORK_NAME}`.int_contract_storage_state_by_block ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_contract_storage_state_by_block_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_contract_storage_state_by_block_local,
    cityHash64(block_number)
);

-- Hourly contract storage state by block (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_by_block_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour' CODEC(ZSTD(1)),
    `active_contracts` Int64 COMMENT 'Cumulative count of contracts with at least one active slot at end of hour' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`)
COMMENT 'Contract storage state metrics aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_by_block_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_contract_storage_state_by_block_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_contract_storage_state_by_block_hourly_local,
    cityHash64(`hour_start_date_time`)
);

-- Daily contract storage state by block (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_by_block_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day' CODEC(ZSTD(1)),
    `active_contracts` Int64 COMMENT 'Cumulative count of contracts with at least one active slot at end of day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (`day_start_date`)
COMMENT 'Contract storage state metrics aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_by_block_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_contract_storage_state_by_block_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_contract_storage_state_by_block_daily_local,
    cityHash64(`day_start_date`)
);

-- Hourly contract storage state by address (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_by_address_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of hour' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of hour' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (address, `hour_start_date_time`)
COMMENT 'Contract storage state metrics per address aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_by_address_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_contract_storage_state_by_address_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_contract_storage_state_by_address_hourly_local,
    cityHash64(address, `hour_start_date_time`)
);

-- Daily contract storage state by address (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_by_address_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `address` String COMMENT 'The contract address' CODEC(ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `active_slots` Int64 COMMENT 'Cumulative count of active storage slots at end of day' CODEC(ZSTD(1)),
    `effective_bytes` Int64 COMMENT 'Cumulative sum of effective bytes at end of day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (address, `day_start_date`)
COMMENT 'Contract storage state metrics per address aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_contract_storage_state_by_address_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_contract_storage_state_by_address_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_contract_storage_state_by_address_daily_local,
    cityHash64(address, `day_start_date`)
);
