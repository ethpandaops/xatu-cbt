-- Restore the comments set in 032_int_execution_block_by_date.up.sql, 054_fct_execution_stats.up.sql and 076_receipt_size.up.sql.

ALTER TABLE fct_execution_transactions_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly aggregated execution layer transaction counts with cumulative totals and per-block statistics';

ALTER TABLE fct_execution_transactions_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the hour period',
    COMMENT COLUMN `block_count` 'Number of blocks in this hour';

ALTER TABLE fct_execution_transactions_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly aggregated execution layer transaction counts with cumulative totals and per-block statistics';

ALTER TABLE fct_execution_transactions_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the hour period',
    COMMENT COLUMN `block_count` 'Number of blocks in this hour';

ALTER TABLE fct_execution_transactions_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily aggregated execution layer transaction counts with cumulative totals and per-block statistics';

ALTER TABLE fct_execution_transactions_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'Start of the day period',
    COMMENT COLUMN `block_count` 'Number of blocks in this day';

ALTER TABLE fct_execution_transactions_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily aggregated execution layer transaction counts with cumulative totals and per-block statistics';

ALTER TABLE fct_execution_transactions_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'Start of the day period',
    COMMENT COLUMN `block_count` 'Number of blocks in this day';

ALTER TABLE int_execution_block_by_date_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution blocks ordered by timestamp for efficient date range lookups';

ALTER TABLE int_execution_block_by_date ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Execution blocks ordered by timestamp for efficient date range lookups';

ALTER TABLE int_transaction_receipt_size_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Per-transaction exact RLP-encoded receipt size derived from logs and transaction data.';

ALTER TABLE int_transaction_receipt_size ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Per-transaction exact RLP-encoded receipt size derived from logs and transaction data.';
