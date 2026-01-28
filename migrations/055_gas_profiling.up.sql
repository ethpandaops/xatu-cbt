-- =============================================================================
-- int_transaction_opcode_gas
-- =============================================================================

-- Local table for opcode-level gas aggregation per transaction
CREATE TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Transaction identifiers
    `block_number` UInt64 COMMENT 'The block number containing the transaction' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'The index of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

    -- Opcode aggregation
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `count` UInt64 COMMENT 'Number of times this opcode was executed in the transaction' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Gas consumed by this opcode. sum(gas) = transaction executed gas' CODEC(ZSTD(1)),
    `gas_cumulative` UInt64 COMMENT 'For CALL opcodes: includes all descendant frame gas. For others: same as gas' CODEC(ZSTD(1)),

    -- Call depth metrics
    `min_depth` UInt64 COMMENT 'Minimum call stack depth for this opcode' CODEC(ZSTD(1)),
    `max_depth` UInt64 COMMENT 'Maximum call stack depth for this opcode' CODEC(ZSTD(1)),

    -- Error tracking
    `error_count` UInt64 COMMENT 'Number of times this opcode resulted in an error' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, opcode, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated opcode-level gas usage per transaction. Source: canonical_execution_transaction_structlog';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_opcode_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_transaction_opcode_gas_local,
    cityHash64(block_number, transaction_hash)
);

-- Projection for opcode-first queries (e.g., "how much gas did SLOAD use across blocks?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number, transaction_hash)
);

-- Projection for transaction-first queries (e.g., "what opcodes did this tx use?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_transaction (
    SELECT *
    ORDER BY (transaction_hash, opcode)
);

-- =============================================================================
-- int_transaction_call_frame
-- =============================================================================

CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_local ON CLUSTER '{cluster}' (
  -- Metadata
  `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

  -- Block and transaction identifiers
  `block_number` UInt64 COMMENT 'The block number containing this transaction' CODEC(DoubleDelta, ZSTD(1)),
  `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
  `transaction_index` UInt32 COMMENT 'Position of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

  -- Call frame identifiers
  `call_frame_id` UInt32 COMMENT 'Sequential frame ID within the transaction (0 = root)' CODEC(DoubleDelta, ZSTD(1)),
  `parent_call_frame_id` Nullable(UInt32) COMMENT 'Parent frame ID (NULL for root frame)' CODEC(ZSTD(1)),
  `depth` UInt32 COMMENT 'Call depth (0 = root transaction execution)' CODEC(DoubleDelta, ZSTD(1)),

  -- Call information
  `target_address` Nullable(String) COMMENT 'Contract address being called (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
  `call_type` LowCardinality(String) COMMENT 'Type of call opcode (CALL, DELEGATECALL, STATICCALL, CALLCODE, CREATE, CREATE2)' CODEC(ZSTD(1)),
  `function_selector` Nullable(String) COMMENT 'Function selector (first 4 bytes of call input, hex encoded with 0x prefix). Populated for all frames from traces.' CODEC(ZSTD(1)),

  -- Aggregated metrics
  `opcode_count` UInt64 COMMENT 'Number of opcodes executed in this frame' CODEC(ZSTD(1)),
  `error_count` UInt64 COMMENT 'Number of opcodes that resulted in errors' CODEC(ZSTD(1)),

  -- Gas metrics (see transformation SQL for full gas model explanation)
  `gas` UInt64 COMMENT 'Gas consumed by this frame only, excludes child frames. sum(gas) = EVM execution gas. This is "self" gas in flame graphs.' CODEC(ZSTD(1)),
  `gas_cumulative` UInt64 COMMENT 'Gas consumed by this frame + all descendants. Root frame value = total EVM execution gas.' CODEC(ZSTD(1)),
  `gas_refund` Nullable(UInt64) COMMENT 'Total accumulated refund. Only populated for root frame, only for successful txs (refund not applied on failure).' CODEC(ZSTD(1)),
  `intrinsic_gas` Nullable(UInt64) COMMENT 'Intrinsic tx cost (21000 + calldata). Only populated for root frame of successful txs.' CODEC(ZSTD(1)),
  `receipt_gas_used` Nullable(UInt64) COMMENT 'Actual gas used from transaction receipt. Only populated for root frame (call_frame_id=0). Source of truth for total gas display.' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
  '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
  '{replica}',
  updated_date_time
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, call_frame_id)
SETTINGS
  deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated call frame activity per transaction for call tree analysis';

CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_call_frame_local
ENGINE = Distributed(
  '{cluster}',
  '${NETWORK_NAME}',
  int_transaction_call_frame_local,
  cityHash64(block_number, transaction_hash)
);

-- Projection for transaction lookups without block_number
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_transaction (
  SELECT *
  ORDER BY (transaction_hash, call_frame_id)
);

-- =============================================================================
-- int_transaction_call_frame_opcode_gas
-- =============================================================================

-- Local table for opcode-level gas aggregation per call frame within transactions
-- This table enables per-frame opcode breakdown, answering "which opcodes did frame N execute?"
CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Transaction identifiers
    `block_number` UInt64 COMMENT 'The block number containing the transaction' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `transaction_index` UInt32 COMMENT 'The index of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

    -- Call frame identifier
    `call_frame_id` UInt32 COMMENT 'Sequential frame ID within transaction (0 = root)' CODEC(DoubleDelta, ZSTD(1)),

    -- Opcode aggregation
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `count` UInt64 COMMENT 'Number of times this opcode was executed in this frame' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Gas consumed by this opcode in this frame. sum(gas) = frame gas' CODEC(ZSTD(1)),
    `gas_cumulative` UInt64 COMMENT 'For CALL opcodes: includes all descendant frame gas. For others: same as gas' CODEC(ZSTD(1)),

    -- Error tracking
    `error_count` UInt64 COMMENT 'Number of times this opcode resulted in an error in this frame' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, call_frame_id, opcode, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated opcode-level gas usage per call frame. Enables per-frame opcode analysis.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_transaction_call_frame_opcode_gas_local,
    cityHash64(block_number, transaction_hash)
);

-- Projection for frame-first queries (e.g., "what opcodes did frame 15 execute?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_frame (
    SELECT *
    ORDER BY (transaction_hash, call_frame_id, opcode)
);

-- Projection for opcode-first queries (e.g., "which frames used SLOAD?")
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number, transaction_hash, call_frame_id)
);

-- =============================================================================
-- dim_function_signature
-- =============================================================================

CREATE TABLE `${NETWORK_NAME}`.dim_function_signature_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `selector` String COMMENT 'Function selector (first 4 bytes of keccak256 hash, hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `name` String COMMENT 'Function signature name (e.g., transfer(address,uint256))' CODEC(ZSTD(1)),
    `has_verified_contract` Bool DEFAULT false COMMENT 'Whether this signature comes from a verified contract source' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PRIMARY KEY (selector)
ORDER BY (selector)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Function signature lookup table populated from Sourcify signature database.';

CREATE TABLE `${NETWORK_NAME}`.dim_function_signature ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.dim_function_signature_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_function_signature_local,
    cityHash64(selector)
);

-- =============================================================================
-- int_block_opcode_gas
-- =============================================================================

-- Local table for block-level opcode gas aggregation
-- Aggregates opcode gas data from int_transaction_opcode_gas to block level
CREATE TABLE `${NETWORK_NAME}`.int_block_opcode_gas_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Block identifier
    `block_number` UInt64 COMMENT 'The block number' CODEC(DoubleDelta, ZSTD(1)),

    -- Opcode aggregation
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `count` UInt64 COMMENT 'Total execution count of this opcode across all transactions in the block' CODEC(ZSTD(1)),
    `gas` UInt64 COMMENT 'Total gas consumed by this opcode across all transactions in the block' CODEC(ZSTD(1)),

    -- Error tracking
    `error_count` UInt64 COMMENT 'Number of times this opcode resulted in an error across all transactions' CODEC(ZSTD(1)),

    -- Network
    `meta_network_name` LowCardinality(String) COMMENT 'The name of the network'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, opcode, meta_network_name)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated opcode-level gas usage per block. Derived from int_transaction_opcode_gas.';

-- Distributed table
CREATE TABLE `${NETWORK_NAME}`.int_block_opcode_gas ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_block_opcode_gas_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_block_opcode_gas_local,
    cityHash64(block_number)
);

-- Projection for opcode-first queries (e.g., "how much gas did SLOAD use across blocks?")
ALTER TABLE `${NETWORK_NAME}`.int_block_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number)
);

-- =============================================================================
-- fct_opcode_ops_hourly
-- =============================================================================

-- Hourly opcode execution rate (ops/sec) with statistical aggregations
CREATE TABLE `${NETWORK_NAME}`.fct_opcode_ops_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this hour' CODEC(ZSTD(1)),
    `total_opcode_count` UInt64 COMMENT 'Total opcode executions in this hour' CODEC(ZSTD(1)),
    `total_gas` UInt64 COMMENT 'Total gas consumed by opcodes in this hour' CODEC(ZSTD(1)),
    `total_seconds` UInt32 COMMENT 'Total actual seconds covered by blocks (sum of block time gaps)' CODEC(ZSTD(1)),
    `avg_ops` Float32 COMMENT 'Average opcodes per second using actual block time gaps' CODEC(ZSTD(1)),
    `min_ops` Float32 COMMENT 'Minimum per-block ops/sec' CODEC(ZSTD(1)),
    `max_ops` Float32 COMMENT 'Maximum per-block ops/sec' CODEC(ZSTD(1)),
    `p05_ops` Float32 COMMENT '5th percentile ops/sec' CODEC(ZSTD(1)),
    `p50_ops` Float32 COMMENT '50th percentile (median) ops/sec' CODEC(ZSTD(1)),
    `p95_ops` Float32 COMMENT '95th percentile ops/sec' CODEC(ZSTD(1)),
    `stddev_ops` Float32 COMMENT 'Standard deviation of ops/sec' CODEC(ZSTD(1)),
    `upper_band_ops` Float32 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_ops` Float32 COMMENT 'Lower Bollinger band (avg - 2*stddev)' CODEC(ZSTD(1)),
    `moving_avg_ops` Float32 COMMENT 'Moving average ops/sec (6-hour window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time)
COMMENT 'Hourly aggregated opcode execution rate statistics with percentiles, Bollinger bands, and moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_opcode_ops_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_opcode_ops_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_opcode_ops_hourly_local,
    cityHash64(hour_start_date_time)
);

-- =============================================================================
-- fct_opcode_ops_daily
-- =============================================================================

-- Daily opcode execution rate (ops/sec) with statistical aggregations
CREATE TABLE `${NETWORK_NAME}`.fct_opcode_ops_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of blocks in this day' CODEC(ZSTD(1)),
    `total_opcode_count` UInt64 COMMENT 'Total opcode executions in this day' CODEC(ZSTD(1)),
    `total_gas` UInt64 COMMENT 'Total gas consumed by opcodes in this day' CODEC(ZSTD(1)),
    `total_seconds` UInt32 COMMENT 'Total actual seconds covered by blocks (sum of block time gaps)' CODEC(ZSTD(1)),
    `avg_ops` Float32 COMMENT 'Average opcodes per second using actual block time gaps' CODEC(ZSTD(1)),
    `min_ops` Float32 COMMENT 'Minimum per-block ops/sec' CODEC(ZSTD(1)),
    `max_ops` Float32 COMMENT 'Maximum per-block ops/sec' CODEC(ZSTD(1)),
    `p05_ops` Float32 COMMENT '5th percentile ops/sec' CODEC(ZSTD(1)),
    `p50_ops` Float32 COMMENT '50th percentile (median) ops/sec' CODEC(ZSTD(1)),
    `p95_ops` Float32 COMMENT '95th percentile ops/sec' CODEC(ZSTD(1)),
    `stddev_ops` Float32 COMMENT 'Standard deviation of ops/sec' CODEC(ZSTD(1)),
    `upper_band_ops` Float32 COMMENT 'Upper Bollinger band (avg + 2*stddev)' CODEC(ZSTD(1)),
    `lower_band_ops` Float32 COMMENT 'Lower Bollinger band (avg - 2*stddev)' CODEC(ZSTD(1)),
    `moving_avg_ops` Float32 COMMENT 'Moving average ops/sec (7-day window)' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date)
COMMENT 'Daily aggregated opcode execution rate statistics with percentiles, Bollinger bands, and moving averages';

CREATE TABLE `${NETWORK_NAME}`.fct_opcode_ops_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_opcode_ops_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_opcode_ops_daily_local,
    cityHash64(day_start_date)
);

-- =============================================================================
-- fct_opcode_gas_by_opcode_hourly
-- =============================================================================

-- Hourly per-opcode gas consumption
CREATE TABLE `${NETWORK_NAME}`.fct_opcode_gas_by_opcode_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `block_count` UInt32 COMMENT 'Number of blocks containing this opcode in this hour' CODEC(ZSTD(1)),
    `total_count` UInt64 COMMENT 'Total execution count of this opcode in this hour' CODEC(ZSTD(1)),
    `total_gas` UInt64 COMMENT 'Total gas consumed by this opcode in this hour' CODEC(ZSTD(1)),
    `total_error_count` UInt64 COMMENT 'Total error count for this opcode in this hour' CODEC(ZSTD(1)),
    `avg_count_per_block` Float32 COMMENT 'Average executions per block' CODEC(ZSTD(1)),
    `avg_gas_per_block` Float32 COMMENT 'Average gas per block' CODEC(ZSTD(1)),
    `avg_gas_per_execution` Float32 COMMENT 'Average gas per execution' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time, opcode)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly per-opcode gas consumption for Top Opcodes by Gas charts';

CREATE TABLE `${NETWORK_NAME}`.fct_opcode_gas_by_opcode_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_opcode_gas_by_opcode_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_opcode_gas_by_opcode_hourly_local,
    cityHash64(hour_start_date_time)
);

-- Projection for opcode-first queries (e.g., "how much gas did SLOAD use over time?")
ALTER TABLE `${NETWORK_NAME}`.fct_opcode_gas_by_opcode_hourly_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, hour_start_date_time)
);

-- =============================================================================
-- fct_opcode_gas_by_opcode_daily
-- =============================================================================

-- Daily per-opcode gas consumption
CREATE TABLE `${NETWORK_NAME}`.fct_opcode_gas_by_opcode_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `opcode` LowCardinality(String) COMMENT 'The EVM opcode name (e.g., SLOAD, ADD, CALL)',
    `block_count` UInt32 COMMENT 'Number of blocks containing this opcode in this day' CODEC(ZSTD(1)),
    `total_count` UInt64 COMMENT 'Total execution count of this opcode in this day' CODEC(ZSTD(1)),
    `total_gas` UInt64 COMMENT 'Total gas consumed by this opcode in this day' CODEC(ZSTD(1)),
    `total_error_count` UInt64 COMMENT 'Total error count for this opcode in this day' CODEC(ZSTD(1)),
    `avg_count_per_block` Float32 COMMENT 'Average executions per block' CODEC(ZSTD(1)),
    `avg_gas_per_block` Float32 COMMENT 'Average gas per block' CODEC(ZSTD(1)),
    `avg_gas_per_execution` Float32 COMMENT 'Average gas per execution' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date, opcode)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily per-opcode gas consumption for Top Opcodes by Gas charts';

CREATE TABLE `${NETWORK_NAME}`.fct_opcode_gas_by_opcode_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_opcode_gas_by_opcode_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_opcode_gas_by_opcode_daily_local,
    cityHash64(day_start_date)
);

-- Projection for opcode-first queries (e.g., "how much gas did SLOAD use over time?")
ALTER TABLE `${NETWORK_NAME}`.fct_opcode_gas_by_opcode_daily_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, day_start_date)
);
