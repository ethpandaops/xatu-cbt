-- Restore the comments set in 036_execution_state_size_agg.up.sql and 054_fct_execution_stats.up.sql.

ALTER TABLE fct_execution_gas_used_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly aggregated execution layer gas used statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_gas_used_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the hour period',
    COMMENT COLUMN `block_count` 'Number of blocks in this hour';

ALTER TABLE fct_execution_gas_used_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly aggregated execution layer gas used statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_gas_used_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the hour period',
    COMMENT COLUMN `block_count` 'Number of blocks in this hour';

ALTER TABLE fct_execution_gas_used_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily aggregated execution layer gas used statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_gas_used_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'Start of the day period',
    COMMENT COLUMN `block_count` 'Number of blocks in this day';

ALTER TABLE fct_execution_gas_used_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily aggregated execution layer gas used statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_gas_used_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'Start of the day period',
    COMMENT COLUMN `block_count` 'Number of blocks in this day';

ALTER TABLE fct_execution_gas_limit_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly aggregated execution layer gas limit statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_gas_limit_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the hour period',
    COMMENT COLUMN `block_count` 'Number of blocks in this hour';

ALTER TABLE fct_execution_gas_limit_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly aggregated execution layer gas limit statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_gas_limit_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the hour period',
    COMMENT COLUMN `block_count` 'Number of blocks in this hour';

ALTER TABLE fct_execution_gas_limit_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily aggregated execution layer gas limit statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_gas_limit_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'Start of the day period',
    COMMENT COLUMN `block_count` 'Number of blocks in this day';

ALTER TABLE fct_execution_gas_limit_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily aggregated execution layer gas limit statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_gas_limit_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'Start of the day period',
    COMMENT COLUMN `block_count` 'Number of blocks in this day';

ALTER TABLE fct_execution_state_size_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution layer state size metrics aggregated by hour';

ALTER TABLE fct_execution_state_size_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the hour period';

ALTER TABLE fct_execution_state_size_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution layer state size metrics aggregated by hour';

ALTER TABLE fct_execution_state_size_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the hour period';

ALTER TABLE fct_execution_state_size_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution layer state size metrics aggregated by day';

ALTER TABLE fct_execution_state_size_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'Start of the day period';

ALTER TABLE fct_execution_state_size_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution layer state size metrics aggregated by day';

ALTER TABLE fct_execution_state_size_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'Start of the day period';
