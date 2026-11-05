-- The gas used and gas limit rollups joined canonical_execution_block without a network filter,
-- so a block number that also exists on another network pulled in the block of that network too.
-- They now read the blocks of this network only. These rollups and the state size rollups also
-- skip the genesis block, which on mainnet has timestamp 0 and produced a 1970-01-01 row.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE fct_execution_gas_used_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gas used per canonical block of this network, aggregated by UTC hour, with percentiles, Bollinger bands and a 6-hour moving average. Starts at block 1, the genesis block is excluded';

ALTER TABLE fct_execution_gas_used_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the UTC hour. The first row is the hour of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this hour, genesis excluded';

ALTER TABLE fct_execution_gas_used_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gas used per canonical block of this network, aggregated by UTC hour, with percentiles, Bollinger bands and a 6-hour moving average. Starts at block 1, the genesis block is excluded';

ALTER TABLE fct_execution_gas_used_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the UTC hour. The first row is the hour of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this hour, genesis excluded';

ALTER TABLE fct_execution_gas_used_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gas used per canonical block of this network, aggregated by UTC day, with percentiles, Bollinger bands and a 7-day moving average. Starts at block 1, the genesis block is excluded';

ALTER TABLE fct_execution_gas_used_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'UTC day. The first row is the day of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this day, genesis excluded';

ALTER TABLE fct_execution_gas_used_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gas used per canonical block of this network, aggregated by UTC day, with percentiles, Bollinger bands and a 7-day moving average. Starts at block 1, the genesis block is excluded';

ALTER TABLE fct_execution_gas_used_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'UTC day. The first row is the day of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this day, genesis excluded';

ALTER TABLE fct_execution_gas_limit_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gas limit per canonical block of this network, aggregated by UTC hour, with percentiles, Bollinger bands and a 6-hour moving average. Starts at block 1, the genesis block is excluded';

ALTER TABLE fct_execution_gas_limit_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the UTC hour. The first row is the hour of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this hour, genesis excluded';

ALTER TABLE fct_execution_gas_limit_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gas limit per canonical block of this network, aggregated by UTC hour, with percentiles, Bollinger bands and a 6-hour moving average. Starts at block 1, the genesis block is excluded';

ALTER TABLE fct_execution_gas_limit_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the UTC hour. The first row is the hour of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this hour, genesis excluded';

ALTER TABLE fct_execution_gas_limit_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gas limit per canonical block of this network, aggregated by UTC day, with percentiles, Bollinger bands and a 7-day moving average. Starts at block 1, the genesis block is excluded';

ALTER TABLE fct_execution_gas_limit_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'UTC day. The first row is the day of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this day, genesis excluded';

ALTER TABLE fct_execution_gas_limit_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gas limit per canonical block of this network, aggregated by UTC day, with percentiles, Bollinger bands and a 7-day moving average. Starts at block 1, the genesis block is excluded';

ALTER TABLE fct_execution_gas_limit_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'UTC day. The first row is the day of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this day, genesis excluded';

ALTER TABLE fct_execution_state_size_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution layer state size at the last canonical block of each UTC hour. Starts at the hour of block 1, the genesis block is not given a row of its own';

ALTER TABLE fct_execution_state_size_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the UTC hour. The first row is the hour of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)';

ALTER TABLE fct_execution_state_size_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution layer state size at the last canonical block of each UTC hour. Starts at the hour of block 1, the genesis block is not given a row of its own';

ALTER TABLE fct_execution_state_size_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the UTC hour. The first row is the hour of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)';

ALTER TABLE fct_execution_state_size_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution layer state size at the last canonical block of each UTC day. Starts at the day of block 1, the genesis block is not given a row of its own';

ALTER TABLE fct_execution_state_size_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'UTC day. The first row is the day of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)';

ALTER TABLE fct_execution_state_size_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution layer state size at the last canonical block of each UTC day. Starts at the day of block 1, the genesis block is not given a row of its own';

ALTER TABLE fct_execution_state_size_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'UTC day. The first row is the day of block 1, never 1970-01-01 (the mainnet genesis block has timestamp 0)';
