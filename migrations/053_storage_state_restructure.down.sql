-- ============================================================================
-- Rollback: Drop all tables created in migration 053
-- ============================================================================

-- Drop distributed tables first, then local tables
-- Tables must be dropped in reverse order of creation

-- ============================================================================
-- DROP SLOT-LEVEL TABLES CREATED IN 053
-- ============================================================================

-- fct_storage_slot_state_with_expiry_by_address_daily
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_daily_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state_with_expiry_by_address_hourly
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_address_hourly_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state_with_expiry_by_block_daily
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_daily_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state_with_expiry_by_block_hourly
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_block_hourly_local ON CLUSTER '{cluster}';

-- int_storage_slot_state_with_expiry_by_block
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_block ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_block_local ON CLUSTER '{cluster}';

-- int_storage_slot_state_with_expiry_by_address
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_address ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_by_address_local ON CLUSTER '{cluster}';

-- int_storage_slot_state_with_expiry
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_with_expiry_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state_by_address_daily
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_by_address_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_by_address_daily_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state_by_address_hourly
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_by_address_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_by_address_hourly_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state_by_block_daily
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_by_block_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_by_block_daily_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state_by_block_hourly
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_by_block_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_by_block_hourly_local ON CLUSTER '{cluster}';

-- int_storage_slot_state_by_block
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_by_block ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_by_block_local ON CLUSTER '{cluster}';

-- int_storage_slot_state_by_address
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_by_address ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_by_address_local ON CLUSTER '{cluster}';

-- int_storage_slot_state
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_state_local ON CLUSTER '{cluster}';

-- ============================================================================
-- RECREATE OLD TABLES (from migrations 037, 039, 050)
-- ============================================================================

-- fct_storage_slot_state (from 037)
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

-- fct_storage_slot_state_hourly (from 039)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_hourly_local ON CLUSTER '{cluster}' (
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

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_hourly_local,
    cityHash64(`hour_start_date_time`)
);

-- fct_storage_slot_state_daily (from 039)
CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_daily_local ON CLUSTER '{cluster}' (
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

CREATE TABLE `${NETWORK_NAME}`.fct_storage_slot_state_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_storage_slot_state_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_storage_slot_state_daily_local,
    cityHash64(`day_start_date`)
);

-- fct_storage_slot_state_with_expiry (from 050)
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

-- fct_storage_slot_state_with_expiry_hourly (from 050)
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

-- fct_storage_slot_state_with_expiry_daily (from 050)
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

-- ============================================================================
-- DROP CONTRACT-LEVEL TABLES CREATED IN 053
-- ============================================================================

-- fct_contract_storage_state_by_address_daily
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_by_address_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_by_address_daily_local ON CLUSTER '{cluster}';

-- fct_contract_storage_state_by_address_hourly
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_by_address_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_by_address_hourly_local ON CLUSTER '{cluster}';

-- fct_contract_storage_state_by_block_daily
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_by_block_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_by_block_daily_local ON CLUSTER '{cluster}';

-- fct_contract_storage_state_by_block_hourly
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_by_block_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_by_block_hourly_local ON CLUSTER '{cluster}';

-- int_contract_storage_state_by_block
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_by_block ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_by_block_local ON CLUSTER '{cluster}';

-- int_contract_storage_state_by_address
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_by_address ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_by_address_local ON CLUSTER '{cluster}';

-- int_contract_storage_state
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_local ON CLUSTER '{cluster}';

-- int_contract_storage_state_with_expiry
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_local ON CLUSTER '{cluster}';

-- fct_contract_storage_state_with_expiry_by_address_daily
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_daily_local ON CLUSTER '{cluster}';

-- fct_contract_storage_state_with_expiry_by_address_hourly
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_address_hourly_local ON CLUSTER '{cluster}';

-- fct_contract_storage_state_with_expiry_by_block_daily
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_daily_local ON CLUSTER '{cluster}';

-- fct_contract_storage_state_with_expiry_by_block_hourly
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_contract_storage_state_with_expiry_by_block_hourly_local ON CLUSTER '{cluster}';

-- int_contract_storage_state_with_expiry_by_block
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_block ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_block_local ON CLUSTER '{cluster}';

-- int_contract_storage_state_with_expiry_by_address
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_address ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_state_with_expiry_by_address_local ON CLUSTER '{cluster}';

-- int_contract_storage_reactivation_24m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_24m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_24m_local ON CLUSTER '{cluster}';

-- int_contract_storage_expiry_24m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_24m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_24m_local ON CLUSTER '{cluster}';

-- int_contract_storage_reactivation_18m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_18m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_18m_local ON CLUSTER '{cluster}';

-- int_contract_storage_expiry_18m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_18m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_18m_local ON CLUSTER '{cluster}';

-- int_contract_storage_reactivation_12m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_12m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_12m_local ON CLUSTER '{cluster}';

-- int_contract_storage_expiry_12m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_12m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_12m_local ON CLUSTER '{cluster}';

-- int_contract_storage_reactivation_6m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_6m_local ON CLUSTER '{cluster}';

-- int_contract_storage_expiry_6m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_6m_local ON CLUSTER '{cluster}';

-- int_contract_storage_reactivation_1m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_1m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_reactivation_1m_local ON CLUSTER '{cluster}';

-- int_contract_storage_expiry_1m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_1m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_expiry_1m_local ON CLUSTER '{cluster}';

-- helper_contract_storage_next_touch_latest_state
DROP TABLE IF EXISTS `${NETWORK_NAME}`.helper_contract_storage_next_touch_latest_state ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.helper_contract_storage_next_touch_latest_state_local ON CLUSTER '{cluster}';

-- int_contract_storage_next_touch
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_next_touch ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_storage_next_touch_local ON CLUSTER '{cluster}';

-- dim_contract_owner
DROP TABLE IF EXISTS `${NETWORK_NAME}`.dim_contract_owner ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.dim_contract_owner_local ON CLUSTER '{cluster}';

-- fct_storage_slot_top_100_by_bytes
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_top_100_by_bytes ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_top_100_by_bytes_local ON CLUSTER '{cluster}';

-- fct_storage_slot_top_100_by_slots
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_top_100_by_slots ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_top_100_by_slots_local ON CLUSTER '{cluster}';
