-- The transactions rollups skip the genesis block like the gas and state size rollups (147), so no rollup
-- counts block 0, whose timestamp is 0 on mainnet and the genesis time on other networks.
-- int_execution_block_by_date and int_transaction_receipt_size now wait until Cannon has written a range
-- completely before reading it, instead of reading whatever was there.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE fct_execution_transactions_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Transactions per canonical block of this network, aggregated by UTC hour, with cumulative totals and per-block statistics. The genesis block is excluded';

ALTER TABLE fct_execution_transactions_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the UTC hour',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this hour, genesis excluded';

ALTER TABLE fct_execution_transactions_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Transactions per canonical block of this network, aggregated by UTC hour, with cumulative totals and per-block statistics. The genesis block is excluded';

ALTER TABLE fct_execution_transactions_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `hour_start_date_time` 'Start of the UTC hour',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this hour, genesis excluded';

ALTER TABLE fct_execution_transactions_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Transactions per canonical block of this network, aggregated by UTC day, with cumulative totals and per-block statistics. The genesis block is excluded';

ALTER TABLE fct_execution_transactions_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'UTC day',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this day, genesis excluded';

ALTER TABLE fct_execution_transactions_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Transactions per canonical block of this network, aggregated by UTC day, with cumulative totals and per-block statistics. The genesis block is excluded';

ALTER TABLE fct_execution_transactions_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `day_start_date` 'UTC day',
    COMMENT COLUMN `block_count` 'Number of canonical blocks of this network in this day, genesis excluded';

ALTER TABLE int_execution_block_by_date_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Block number and block time of every canonical execution block of this network, for date range lookups. A range is only written once canonical_execution_block holds every block in it';

ALTER TABLE int_execution_block_by_date ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Block number and block time of every canonical execution block of this network, for date range lookups. A range is only written once canonical_execution_block holds every block in it';

ALTER TABLE int_transaction_receipt_size_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Per-transaction exact RLP-encoded receipt size derived from logs and transaction data. A range is only written once no block in it is missing transactions, and no block has a hole in its transaction or log indexes';

ALTER TABLE int_transaction_receipt_size ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Per-transaction exact RLP-encoded receipt size derived from logs and transaction data. A range is only written once no block in it is missing transactions, and no block has a hole in its transaction or log indexes';
