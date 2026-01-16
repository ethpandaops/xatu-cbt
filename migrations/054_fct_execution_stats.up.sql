-- fct_execution_tps_hourly
CREATE TABLE `${NETWORK_NAME}`.fct_execution_tps_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this hour' CODEC(ZSTD(1)),
    `total_transactions` UInt64 COMMENT 'Total transactions in this hour' CODEC(ZSTD(1)),
    `total_seconds` UInt32 COMMENT 'Total actual seconds covered by blocks (sum of block time gaps)' CODEC(ZSTD(1)),
    `avg_tps` Float32 COMMENT 'Average TPS using actual block time gaps' CODEC(ZSTD(1)),
    `min_tps` Float32 COMMENT 'Minimum per-block TPS' CODEC(ZSTD(1)),
    `max_tps` Float32 COMMENT 'Maximum per-block TPS' CODEC(ZSTD(1)),
    `p05_tps` Float32 COMMENT '5th percentile TPS' CODEC(ZSTD(1)),
    `p50_tps` Float32 COMMENT '50th percentile (median) TPS' CODEC(ZSTD(1)),
    `p95_tps` Float32 COMMENT '95th percentile TPS' CODEC(ZSTD(1)),
    `stddev_tps` Float32 COMMENT 'Standard deviation of TPS' CODEC(ZSTD(1)),
    `upper_band_tps` Float32 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_tps` Float32 COMMENT 'Lower Bollinger band (avg - 2*stddev), can be negative during high volatility' CODEC(ZSTD(1)),
    `moving_avg_tps` Float32 COMMENT 'Moving average TPS (6-hour window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time)
COMMENT 'Hourly aggregated execution layer TPS statistics with percentiles, Bollinger bands, and moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_tps_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_tps_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_tps_hourly_local,
    cityHash64(hour_start_date_time)
);

-- fct_execution_tps_daily
CREATE TABLE `${NETWORK_NAME}`.fct_execution_tps_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this day' CODEC(ZSTD(1)),
    `total_transactions` UInt64 COMMENT 'Total transactions in this day' CODEC(ZSTD(1)),
    `total_seconds` UInt32 COMMENT 'Total actual seconds covered by blocks (sum of block time gaps)' CODEC(ZSTD(1)),
    `avg_tps` Float32 COMMENT 'Average TPS using actual block time gaps' CODEC(ZSTD(1)),
    `min_tps` Float32 COMMENT 'Minimum per-block TPS' CODEC(ZSTD(1)),
    `max_tps` Float32 COMMENT 'Maximum per-block TPS' CODEC(ZSTD(1)),
    `p05_tps` Float32 COMMENT '5th percentile TPS' CODEC(ZSTD(1)),
    `p50_tps` Float32 COMMENT '50th percentile (median) TPS' CODEC(ZSTD(1)),
    `p95_tps` Float32 COMMENT '95th percentile TPS' CODEC(ZSTD(1)),
    `stddev_tps` Float32 COMMENT 'Standard deviation of TPS' CODEC(ZSTD(1)),
    `upper_band_tps` Float32 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_tps` Float32 COMMENT 'Lower Bollinger band (avg - 2*stddev), can be negative during high volatility' CODEC(ZSTD(1)),
    `moving_avg_tps` Float32 COMMENT 'Moving average TPS (7-day window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Daily aggregated execution layer TPS statistics with percentiles, Bollinger bands, and moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_tps_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_tps_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_tps_daily_local,
    cityHash64(day_start_date)
);

-- fct_execution_gas_used_hourly
CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_used_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this hour' CODEC(ZSTD(1)),
    `total_gas_used` UInt64 COMMENT 'Total gas used in this hour' CODEC(ZSTD(1)),
    `cumulative_gas_used` UInt64 COMMENT 'Cumulative gas used since genesis' CODEC(ZSTD(1)),
    `avg_gas_used` UInt64 COMMENT 'Average gas used per block' CODEC(ZSTD(1)),
    `min_gas_used` UInt64 COMMENT 'Minimum gas used in a block' CODEC(ZSTD(1)),
    `max_gas_used` UInt64 COMMENT 'Maximum gas used in a block' CODEC(ZSTD(1)),
    `p05_gas_used` UInt64 COMMENT '5th percentile gas used' CODEC(ZSTD(1)),
    `p50_gas_used` UInt64 COMMENT '50th percentile (median) gas used' CODEC(ZSTD(1)),
    `p95_gas_used` UInt64 COMMENT '95th percentile gas used' CODEC(ZSTD(1)),
    `stddev_gas_used` UInt64 COMMENT 'Standard deviation of gas used' CODEC(ZSTD(1)),
    `upper_band_gas_used` UInt64 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_gas_used` Int64 COMMENT 'Lower Bollinger band (avg - 2*stddev), can be negative during high volatility' CODEC(ZSTD(1)),
    `moving_avg_gas_used` UInt64 COMMENT 'Moving average gas used (6-hour window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time)
COMMENT 'Hourly aggregated execution layer gas used statistics with percentiles, Bollinger bands, and moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_used_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_gas_used_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_gas_used_hourly_local,
    cityHash64(hour_start_date_time)
);

-- fct_execution_gas_used_daily
CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_used_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this day' CODEC(ZSTD(1)),
    `total_gas_used` UInt64 COMMENT 'Total gas used in this day' CODEC(ZSTD(1)),
    `cumulative_gas_used` UInt64 COMMENT 'Cumulative gas used since genesis' CODEC(ZSTD(1)),
    `avg_gas_used` UInt64 COMMENT 'Average gas used per block' CODEC(ZSTD(1)),
    `min_gas_used` UInt64 COMMENT 'Minimum gas used in a block' CODEC(ZSTD(1)),
    `max_gas_used` UInt64 COMMENT 'Maximum gas used in a block' CODEC(ZSTD(1)),
    `p05_gas_used` UInt64 COMMENT '5th percentile gas used' CODEC(ZSTD(1)),
    `p50_gas_used` UInt64 COMMENT '50th percentile (median) gas used' CODEC(ZSTD(1)),
    `p95_gas_used` UInt64 COMMENT '95th percentile gas used' CODEC(ZSTD(1)),
    `stddev_gas_used` UInt64 COMMENT 'Standard deviation of gas used' CODEC(ZSTD(1)),
    `upper_band_gas_used` UInt64 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_gas_used` Int64 COMMENT 'Lower Bollinger band (avg - 2*stddev), can be negative during high volatility' CODEC(ZSTD(1)),
    `moving_avg_gas_used` UInt64 COMMENT 'Moving average gas used (7-day window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Daily aggregated execution layer gas used statistics with percentiles, Bollinger bands, and moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_used_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_gas_used_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_gas_used_daily_local,
    cityHash64(day_start_date)
);

-- fct_execution_gas_limit_hourly
CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_limit_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this hour' CODEC(ZSTD(1)),
    `total_gas_limit` UInt64 COMMENT 'Total gas limit in this hour' CODEC(ZSTD(1)),
    `avg_gas_limit` UInt64 COMMENT 'Average gas limit per block' CODEC(ZSTD(1)),
    `min_gas_limit` UInt64 COMMENT 'Minimum gas limit in a block' CODEC(ZSTD(1)),
    `max_gas_limit` UInt64 COMMENT 'Maximum gas limit in a block' CODEC(ZSTD(1)),
    `p05_gas_limit` UInt64 COMMENT '5th percentile gas limit' CODEC(ZSTD(1)),
    `p50_gas_limit` UInt64 COMMENT '50th percentile (median) gas limit' CODEC(ZSTD(1)),
    `p95_gas_limit` UInt64 COMMENT '95th percentile gas limit' CODEC(ZSTD(1)),
    `stddev_gas_limit` UInt64 COMMENT 'Standard deviation of gas limit' CODEC(ZSTD(1)),
    `upper_band_gas_limit` UInt64 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_gas_limit` Int64 COMMENT 'Lower Bollinger band (avg - 2*stddev), can be negative during high volatility' CODEC(ZSTD(1)),
    `moving_avg_gas_limit` UInt64 COMMENT 'Moving average gas limit (6-hour window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time)
COMMENT 'Hourly aggregated execution layer gas limit statistics with percentiles, Bollinger bands, and moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_limit_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_gas_limit_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_gas_limit_hourly_local,
    cityHash64(hour_start_date_time)
);

-- fct_execution_gas_limit_daily
CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_limit_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this day' CODEC(ZSTD(1)),
    `total_gas_limit` UInt64 COMMENT 'Total gas limit in this day' CODEC(ZSTD(1)),
    `avg_gas_limit` UInt64 COMMENT 'Average gas limit per block' CODEC(ZSTD(1)),
    `min_gas_limit` UInt64 COMMENT 'Minimum gas limit in a block' CODEC(ZSTD(1)),
    `max_gas_limit` UInt64 COMMENT 'Maximum gas limit in a block' CODEC(ZSTD(1)),
    `p05_gas_limit` UInt64 COMMENT '5th percentile gas limit' CODEC(ZSTD(1)),
    `p50_gas_limit` UInt64 COMMENT '50th percentile (median) gas limit' CODEC(ZSTD(1)),
    `p95_gas_limit` UInt64 COMMENT '95th percentile gas limit' CODEC(ZSTD(1)),
    `stddev_gas_limit` UInt64 COMMENT 'Standard deviation of gas limit' CODEC(ZSTD(1)),
    `upper_band_gas_limit` UInt64 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_gas_limit` Int64 COMMENT 'Lower Bollinger band (avg - 2*stddev), can be negative during high volatility' CODEC(ZSTD(1)),
    `moving_avg_gas_limit` UInt64 COMMENT 'Moving average gas limit (7-day window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Daily aggregated execution layer gas limit statistics with percentiles, Bollinger bands, and moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_limit_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_gas_limit_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_gas_limit_daily_local,
    cityHash64(day_start_date)
);

-- fct_execution_gas_limit_signalling_hourly (rolling 7-day window with Map schema)
CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_limit_signalling_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `gas_limit_band_counts` Map(String, UInt32) COMMENT 'Map of gas limit band (1M increments) to validator count from rolling 7-day window' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time)
COMMENT 'Hourly snapshots of validator gas limit signalling using rolling 7-day window';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_limit_signalling_hourly ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime,
    `hour_start_date_time` DateTime,
    `gas_limit_band_counts` Map(String, UInt32)
) ENGINE = Distributed('{cluster}', '${NETWORK_NAME}', fct_execution_gas_limit_signalling_hourly_local, cityHash64(hour_start_date_time));

-- fct_execution_gas_limit_signalling_daily (rolling 7-day window with Map schema)
CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_limit_signalling_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `gas_limit_band_counts` Map(String, UInt32) COMMENT 'Map of gas limit band (1M increments) to validator count from rolling 7-day window' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Daily snapshots of validator gas limit signalling using rolling 7-day window';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_gas_limit_signalling_daily ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime,
    `day_start_date` Date,
    `gas_limit_band_counts` Map(String, UInt32)
) ENGINE = Distributed('{cluster}', '${NETWORK_NAME}', fct_execution_gas_limit_signalling_daily_local, cityHash64(day_start_date));

-- fct_execution_transactions_hourly
CREATE TABLE `${NETWORK_NAME}`.fct_execution_transactions_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this hour' CODEC(ZSTD(1)),
    `total_transactions` UInt64 COMMENT 'Total transactions in this hour' CODEC(ZSTD(1)),
    `cumulative_transactions` UInt64 COMMENT 'Cumulative transaction count since genesis' CODEC(ZSTD(1)),
    `avg_txn_per_block` Float32 COMMENT 'Average transactions per block' CODEC(ZSTD(1)),
    `min_txn_per_block` UInt32 COMMENT 'Minimum transactions in a block' CODEC(ZSTD(1)),
    `max_txn_per_block` UInt32 COMMENT 'Maximum transactions in a block' CODEC(ZSTD(1)),
    `p50_txn_per_block` UInt32 COMMENT '50th percentile (median) transactions per block' CODEC(ZSTD(1)),
    `p95_txn_per_block` UInt32 COMMENT '95th percentile transactions per block' CODEC(ZSTD(1)),
    `p05_txn_per_block` UInt32 COMMENT '5th percentile transactions per block' CODEC(ZSTD(1)),
    `stddev_txn_per_block` Float32 COMMENT 'Standard deviation of transactions per block' CODEC(ZSTD(1)),
    `upper_band_txn_per_block` Float32 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_txn_per_block` Float32 COMMENT 'Lower Bollinger band (avg - 2*stddev)' CODEC(ZSTD(1)),
    `moving_avg_txn_per_block` Float32 COMMENT 'Moving average transactions per block (6-hour window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time)
COMMENT 'Hourly aggregated execution layer transaction counts with cumulative totals and per-block statistics';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_transactions_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_transactions_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_transactions_hourly_local,
    cityHash64(hour_start_date_time)
);

-- fct_execution_transactions_daily
CREATE TABLE `${NETWORK_NAME}`.fct_execution_transactions_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this day' CODEC(ZSTD(1)),
    `total_transactions` UInt64 COMMENT 'Total transactions in this day' CODEC(ZSTD(1)),
    `cumulative_transactions` UInt64 COMMENT 'Cumulative transaction count since genesis' CODEC(ZSTD(1)),
    `avg_txn_per_block` Float32 COMMENT 'Average transactions per block' CODEC(ZSTD(1)),
    `min_txn_per_block` UInt32 COMMENT 'Minimum transactions in a block' CODEC(ZSTD(1)),
    `max_txn_per_block` UInt32 COMMENT 'Maximum transactions in a block' CODEC(ZSTD(1)),
    `p50_txn_per_block` UInt32 COMMENT '50th percentile (median) transactions per block' CODEC(ZSTD(1)),
    `p95_txn_per_block` UInt32 COMMENT '95th percentile transactions per block' CODEC(ZSTD(1)),
    `p05_txn_per_block` UInt32 COMMENT '5th percentile transactions per block' CODEC(ZSTD(1)),
    `stddev_txn_per_block` Float32 COMMENT 'Standard deviation of transactions per block' CODEC(ZSTD(1)),
    `upper_band_txn_per_block` Float32 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_txn_per_block` Float32 COMMENT 'Lower Bollinger band (avg - 2*stddev)' CODEC(ZSTD(1)),
    `moving_avg_txn_per_block` Float32 COMMENT 'Moving average transactions per block (7-day window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Daily aggregated execution layer transaction counts with cumulative totals and per-block statistics';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_transactions_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_transactions_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_transactions_daily_local,
    cityHash64(day_start_date)
);
