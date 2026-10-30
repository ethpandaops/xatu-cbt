CREATE TABLE int_engine_new_payload_opcode_gram_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour, by block timestamp' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification the timings come from (e.g., eip7870-block-builder)',
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation (e.g., Geth, Nethermind, Besu, Reth)',
    `meta_execution_version` LowCardinality(String) COMMENT 'Execution client version string',
    `block_count` UInt32 COMMENT 'Blocks in the regression after trimming outliers' CODEC(ZSTD(1)),
    `trimmed_block_count` UInt32 COMMENT 'Blocks dropped as outliers (above the hourly P99 duration, or above 3x the hourly median ms per Mgas)' CODEC(ZSTD(1)),
    `min_block_number` UInt64 COMMENT 'Lowest execution block number in the hour' CODEC(DoubleDelta, ZSTD(1)),
    `max_block_number` UInt64 COMMENT 'Highest execution block number in the hour' CODEC(DoubleDelta, ZSTD(1)),
    `feature_names` Array(LowCardinality(String)) COMMENT 'Regression features, in order: INTERCEPT (1 per block), TX_COUNT (transactions / 100), NON_OPCODE_GAS (gas used minus opcode gas, in Mgas), then gas per opcode group in Mgas',
    `xtx` Array(Float64) COMMENT 'Upper triangle (row-major, including the diagonal) of X^T X, where X is blocks by feature_names' CODEC(ZSTD(1)),
    `xty` Array(Float64) COMMENT 'X^T y, where y is the median engine_newPayload duration in milliseconds across the node class' CODEC(ZSTD(1)),
    `yty` Float64 COMMENT 'y^T y, the sum of squared durations in ms^2' CODEC(ZSTD(1)),
    `opcode_count` Array(UInt64) COMMENT 'Executions per opcode group, aligned with feature_names after the first three features' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`, `node_class`, `meta_execution_implementation`, `meta_execution_version`)
COMMENT 'Hourly least-squares sufficient statistics for regressing engine_newPayload duration on gas per opcode group, by execution client version';

CREATE TABLE int_engine_new_payload_opcode_gram_hourly ON CLUSTER '{cluster}' AS int_engine_new_payload_opcode_gram_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_engine_new_payload_opcode_gram_hourly_local,
    cityHash64(`hour_start_date_time`, `node_class`, `meta_execution_implementation`, `meta_execution_version`)
);

CREATE TABLE fct_opcode_cost_by_el_client_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by block timestamp' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification the timings come from (e.g., eip7870-block-builder)',
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation (e.g., Geth, Nethermind, Besu, Reth)',
    `opcode_group` LowCardinality(String) COMMENT 'Opcode, or opcode group (LOG, ARITHMETIC, CONTROL, ENVIRONMENT, MEMORY, STACK)',
    `meta_execution_versions` Array(String) COMMENT 'Client versions that ran during the day, most blocks first' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Block observations in the fit' CODEC(ZSTD(1)),
    `hour_count` UInt32 COMMENT 'Hours with data in the fit' CODEC(ZSTD(1)),
    `gas_share` Float64 COMMENT 'Share of all opcode and non-opcode gas spent on this opcode group' CODEC(ZSTD(1)),
    `gas_per_op` Float64 COMMENT 'Average gas charged per execution of this opcode group' CODEC(ZSTD(1)),
    `ns_per_gas` Float64 COMMENT 'Fitted execution time per unit of gas for this opcode group, in nanoseconds' CODEC(ZSTD(1)),
    `ns_per_op` Float64 COMMENT 'Fitted execution time per execution of this opcode group, in nanoseconds' CODEC(ZSTD(1)),
    `ns_per_op_lower` Float64 COMMENT '5th percentile of ns_per_op over an hourly bootstrap' CODEC(ZSTD(1)),
    `ns_per_op_upper` Float64 COMMENT '95th percentile of ns_per_op over an hourly bootstrap' CODEC(ZSTD(1)),
    `price_ratio` Float64 COMMENT 'ns_per_gas divided by the client average ns per gas, where 1 means gas prices this opcode group in line with the rest of execution' CODEC(ZSTD(1)),
    `price_ratio_lower` Float64 COMMENT '5th percentile of price_ratio over an hourly bootstrap' CODEC(ZSTD(1)),
    `price_ratio_upper` Float64 COMMENT '95th percentile of price_ratio over an hourly bootstrap' CODEC(ZSTD(1)),
    `client_ns_per_gas` Float64 COMMENT 'Client average execution time per unit of gas, in nanoseconds' CODEC(ZSTD(1)),
    `overhead_ms` Float64 COMMENT 'Fitted fixed cost per block, in milliseconds' CODEC(ZSTD(1)),
    `per_tx_us` Float64 COMMENT 'Fitted cost per transaction, in microseconds' CODEC(ZSTD(1)),
    `r_squared` Float64 COMMENT 'Share of block duration variance the fit explains' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY (`day_start_date`, `node_class`, `meta_execution_implementation`, `opcode_group`)
COMMENT 'Daily execution cost per opcode group by execution client, fitted from engine_newPayload durations';

CREATE TABLE fct_opcode_cost_by_el_client_daily ON CLUSTER '{cluster}' AS fct_opcode_cost_by_el_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_opcode_cost_by_el_client_daily_local,
    cityHash64(`day_start_date`, `node_class`, `meta_execution_implementation`, `opcode_group`)
);

CREATE TABLE fct_opcode_cost_by_el_client_version_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification the timings come from (e.g., eip7870-block-builder)',
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation (e.g., Geth, Nethermind, Besu, Reth)',
    `meta_execution_version` LowCardinality(String) COMMENT 'Execution client version string',
    `opcode_group` LowCardinality(String) COMMENT 'Opcode, or opcode group (LOG, ARITHMETIC, CONTROL, ENVIRONMENT, MEMORY, STACK)',
    `first_seen_date_time` DateTime COMMENT 'Start of the first hour the version has data' CODEC(DoubleDelta, ZSTD(1)),
    `last_seen_date_time` DateTime COMMENT 'Start of the last hour the version has data' CODEC(DoubleDelta, ZSTD(1)),
    `block_count` UInt32 COMMENT 'Block observations in the fit' CODEC(ZSTD(1)),
    `hour_count` UInt32 COMMENT 'Hours with data in the fit' CODEC(ZSTD(1)),
    `gas_share` Float64 COMMENT 'Share of all opcode and non-opcode gas spent on this opcode group' CODEC(ZSTD(1)),
    `gas_per_op` Float64 COMMENT 'Average gas charged per execution of this opcode group' CODEC(ZSTD(1)),
    `ns_per_gas` Float64 COMMENT 'Fitted execution time per unit of gas for this opcode group, in nanoseconds' CODEC(ZSTD(1)),
    `ns_per_op` Float64 COMMENT 'Fitted execution time per execution of this opcode group, in nanoseconds' CODEC(ZSTD(1)),
    `ns_per_op_lower` Float64 COMMENT '5th percentile of ns_per_op over a 6-hour block bootstrap' CODEC(ZSTD(1)),
    `ns_per_op_upper` Float64 COMMENT '95th percentile of ns_per_op over a 6-hour block bootstrap' CODEC(ZSTD(1)),
    `price_ratio` Float64 COMMENT 'ns_per_gas divided by the client average ns per gas, where 1 means gas prices this opcode group in line with the rest of execution' CODEC(ZSTD(1)),
    `price_ratio_lower` Float64 COMMENT '5th percentile of price_ratio over a 6-hour block bootstrap' CODEC(ZSTD(1)),
    `price_ratio_upper` Float64 COMMENT '95th percentile of price_ratio over a 6-hour block bootstrap' CODEC(ZSTD(1)),
    `client_ns_per_gas` Float64 COMMENT 'Client average execution time per unit of gas, in nanoseconds' CODEC(ZSTD(1)),
    `overhead_ms` Float64 COMMENT 'Fitted fixed cost per block, in milliseconds' CODEC(ZSTD(1)),
    `per_tx_us` Float64 COMMENT 'Fitted cost per transaction, in microseconds' CODEC(ZSTD(1)),
    `r_squared` Float64 COMMENT 'Share of block duration variance the fit explains' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY (`node_class`, `meta_execution_implementation`, `meta_execution_version`, `opcode_group`)
COMMENT 'Execution cost per opcode group for each execution client version, fitted over every hour the version ran';

CREATE TABLE fct_opcode_cost_by_el_client_version ON CLUSTER '{cluster}' AS fct_opcode_cost_by_el_client_version_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_opcode_cost_by_el_client_version_local,
    cityHash64(`node_class`, `meta_execution_implementation`, `meta_execution_version`, `opcode_group`)
);
