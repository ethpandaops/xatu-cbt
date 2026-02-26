-- =============================================================================
-- int_transaction_receipt_size
-- =============================================================================
-- Per-transaction exact RLP-encoded receipt size derived from
-- canonical_execution_logs and canonical_execution_transaction.

CREATE TABLE `${NETWORK_NAME}`.int_transaction_receipt_size_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Transaction identifiers
    `block_number` UInt64 COMMENT 'The block number containing the transaction' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'The index of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

    -- Receipt size
    `receipt_bytes` UInt64 COMMENT 'Exact RLP-encoded receipt size in bytes (matches eth_getTransactionReceipt)' CODEC(ZSTD(1)),

    -- Log metrics
    `log_count` UInt32 COMMENT 'Number of logs emitted by this transaction' CODEC(ZSTD(1)),
    `log_data_bytes` UInt64 COMMENT 'Total raw bytes of log data fields (before RLP encoding)' CODEC(ZSTD(1)),
    `log_topic_count` UInt32 COMMENT 'Total number of topics across all logs' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-transaction exact RLP-encoded receipt size derived from logs and transaction data.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_transaction_receipt_size ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_receipt_size_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_transaction_receipt_size_local,
    cityHash64(block_number, transaction_hash)
);

-- =============================================================================
-- int_block_receipt_size
-- =============================================================================
-- Per-block receipt size totals. Derived from int_transaction_receipt_size.

CREATE TABLE `${NETWORK_NAME}`.int_block_receipt_size_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Block identifier
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),

    -- Receipt size
    `receipt_bytes` UInt64 COMMENT 'Total RLP-encoded receipt bytes across all transactions in the block' CODEC(ZSTD(1)),
    `transaction_count` UInt32 COMMENT 'Number of transactions in the block' CODEC(ZSTD(1)),

    -- Log metrics
    `log_count` UInt32 COMMENT 'Total logs emitted across all transactions in the block' CODEC(ZSTD(1)),
    `log_data_bytes` UInt64 COMMENT 'Total raw bytes of log data fields across all transactions' CODEC(ZSTD(1)),
    `log_topic_count` UInt32 COMMENT 'Total number of topics across all logs in the block' CODEC(ZSTD(1)),

    -- Per-transaction stats within the block
    `avg_receipt_bytes_per_transaction` Float64 COMMENT 'Average receipt bytes per transaction in this block' CODEC(ZSTD(1)),
    `max_receipt_bytes_per_transaction` UInt64 COMMENT 'Largest single transaction receipt in this block' CODEC(ZSTD(1)),
    `p50_receipt_bytes_per_transaction` UInt64 COMMENT '50th percentile of receipt bytes per transaction' CODEC(ZSTD(1)),
    `p95_receipt_bytes_per_transaction` UInt64 COMMENT '95th percentile of receipt bytes per transaction' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-block receipt size totals. Derived from int_transaction_receipt_size.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_block_receipt_size ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_block_receipt_size_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_block_receipt_size_local,
    cityHash64(block_number)
);

-- =============================================================================
-- fct_execution_receipt_size_daily
-- =============================================================================
-- Daily aggregation of receipt size with standard statistics suite.

CREATE TABLE `${NETWORK_NAME}`.fct_execution_receipt_size_daily_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `day_start_date` Date COMMENT 'The start date of the day',

    -- Block and transaction counts
    `block_count` UInt32 COMMENT 'Number of blocks in this day' CODEC(ZSTD(1)),
    `transaction_count` UInt64 COMMENT 'Number of transactions in this day' CODEC(ZSTD(1)),

    -- Receipt size per block
    `total_receipt_bytes` UInt64 COMMENT 'Total receipt bytes across all blocks' CODEC(ZSTD(1)),
    `avg_receipt_bytes_per_block` Float64 COMMENT 'Average total receipt bytes per block' CODEC(ZSTD(1)),
    `min_receipt_bytes_per_block` UInt64 COMMENT 'Minimum total receipt bytes in a single block' CODEC(ZSTD(1)),
    `max_receipt_bytes_per_block` UInt64 COMMENT 'Maximum total receipt bytes in a single block' CODEC(ZSTD(1)),
    `p05_receipt_bytes_per_block` UInt64 COMMENT '5th percentile of total receipt bytes per block' CODEC(ZSTD(1)),
    `p50_receipt_bytes_per_block` UInt64 COMMENT '50th percentile of total receipt bytes per block' CODEC(ZSTD(1)),
    `p95_receipt_bytes_per_block` UInt64 COMMENT '95th percentile of total receipt bytes per block' CODEC(ZSTD(1)),
    `stddev_receipt_bytes_per_block` Float64 COMMENT 'Standard deviation of total receipt bytes per block' CODEC(ZSTD(1)),
    `upper_band_receipt_bytes_per_block` Float64 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_receipt_bytes_per_block` Float64 COMMENT 'Lower Bollinger band (avg - 2*stddev)' CODEC(ZSTD(1)),
    `moving_avg_receipt_bytes_per_block` Float64 COMMENT '7-day moving average of receipt bytes per block' CODEC(ZSTD(1)),

    -- Receipt size per transaction
    `avg_receipt_bytes_per_transaction` Float64 COMMENT 'Average receipt bytes per transaction' CODEC(ZSTD(1)),
    `p50_receipt_bytes_per_transaction` UInt64 COMMENT '50th percentile of receipt bytes per transaction' CODEC(ZSTD(1)),
    `p95_receipt_bytes_per_transaction` UInt64 COMMENT '95th percentile of receipt bytes per transaction' CODEC(ZSTD(1)),

    -- Log metrics
    `total_log_count` UInt64 COMMENT 'Total logs emitted across all transactions' CODEC(ZSTD(1)),
    `avg_log_count_per_transaction` Float64 COMMENT 'Average logs per transaction' CODEC(ZSTD(1)),

    -- Cumulative
    `cumulative_receipt_bytes` UInt64 COMMENT 'Running total of receipt bytes since genesis' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY (day_start_date)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily aggregation of receipt size with standard statistics.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.fct_execution_receipt_size_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_receipt_size_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_receipt_size_daily_local,
    cityHash64(day_start_date)
);

-- =============================================================================
-- fct_execution_receipt_size_hourly
-- =============================================================================
-- Hourly aggregation of receipt size with standard statistics suite.

CREATE TABLE `${NETWORK_NAME}`.fct_execution_receipt_size_hourly_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimension
    `hour_start_date_time` DateTime COMMENT 'The start time of the hour',

    -- Block and transaction counts
    `block_count` UInt32 COMMENT 'Number of blocks in this hour' CODEC(ZSTD(1)),
    `transaction_count` UInt64 COMMENT 'Number of transactions in this hour' CODEC(ZSTD(1)),

    -- Receipt size per block
    `total_receipt_bytes` UInt64 COMMENT 'Total receipt bytes across all blocks' CODEC(ZSTD(1)),
    `avg_receipt_bytes_per_block` Float64 COMMENT 'Average total receipt bytes per block' CODEC(ZSTD(1)),
    `min_receipt_bytes_per_block` UInt64 COMMENT 'Minimum total receipt bytes in a single block' CODEC(ZSTD(1)),
    `max_receipt_bytes_per_block` UInt64 COMMENT 'Maximum total receipt bytes in a single block' CODEC(ZSTD(1)),
    `p05_receipt_bytes_per_block` UInt64 COMMENT '5th percentile of total receipt bytes per block' CODEC(ZSTD(1)),
    `p50_receipt_bytes_per_block` UInt64 COMMENT '50th percentile of total receipt bytes per block' CODEC(ZSTD(1)),
    `p95_receipt_bytes_per_block` UInt64 COMMENT '95th percentile of total receipt bytes per block' CODEC(ZSTD(1)),
    `stddev_receipt_bytes_per_block` Float64 COMMENT 'Standard deviation of total receipt bytes per block' CODEC(ZSTD(1)),
    `upper_band_receipt_bytes_per_block` Float64 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_receipt_bytes_per_block` Float64 COMMENT 'Lower Bollinger band (avg - 2*stddev)' CODEC(ZSTD(1)),
    `moving_avg_receipt_bytes_per_block` Float64 COMMENT '6-hour moving average of receipt bytes per block' CODEC(ZSTD(1)),

    -- Receipt size per transaction
    `avg_receipt_bytes_per_transaction` Float64 COMMENT 'Average receipt bytes per transaction' CODEC(ZSTD(1)),
    `p50_receipt_bytes_per_transaction` UInt64 COMMENT '50th percentile of receipt bytes per transaction' CODEC(ZSTD(1)),
    `p95_receipt_bytes_per_transaction` UInt64 COMMENT '95th percentile of receipt bytes per transaction' CODEC(ZSTD(1)),

    -- Log metrics
    `total_log_count` UInt64 COMMENT 'Total logs emitted across all transactions' CODEC(ZSTD(1)),
    `avg_log_count_per_transaction` Float64 COMMENT 'Average logs per transaction' CODEC(ZSTD(1)),

    -- Cumulative
    `cumulative_receipt_bytes` UInt64 COMMENT 'Running total of receipt bytes since genesis' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY (hour_start_date_time)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly aggregation of receipt size with standard statistics.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.fct_execution_receipt_size_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_receipt_size_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_receipt_size_hourly_local,
    cityHash64(hour_start_date_time)
);
