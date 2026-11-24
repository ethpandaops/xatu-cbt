ALTER TABLE fct_attestation_correctness_by_validator_head_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Attestation correctness by validator for the finalized chain';

ALTER TABLE fct_attestation_correctness_by_validator_head ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Attestation correctness by validator for the finalized chain';

ALTER TABLE int_attestation_attested_canonical_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Attested head of a block for the unfinalized chain.';

ALTER TABLE int_attestation_attested_canonical ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Attested head of a block for the unfinalized chain.';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index';

ALTER TABLE fct_data_column_availability_by_slot_blob_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot, blob index, and column index';

ALTER TABLE fct_data_column_availability_by_slot_blob ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot, blob index, and column index';

ALTER TABLE fct_data_column_availability_by_epoch_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by epoch and column index';

ALTER TABLE fct_data_column_availability_by_epoch ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by epoch and column index';

ALTER TABLE fct_data_column_availability_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by hour and column index';

ALTER TABLE fct_data_column_availability_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by hour and column index';

ALTER TABLE fct_data_column_availability_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by day and column index';

ALTER TABLE fct_data_column_availability_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by day and column index';

ALTER TABLE int_custody_probe_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results per slot with aggregated column indices';

ALTER TABLE int_custody_probe ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results per slot with aggregated column indices';

ALTER TABLE int_custody_probe_order_by_slot_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results per slot with aggregated column indices';

ALTER TABLE int_custody_probe_order_by_slot ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results per slot with aggregated column indices';

ALTER TABLE fct_node_active_last_24h_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Active nodes for the network';

ALTER TABLE fct_node_active_last_24h_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `node_id` 'ID of the node';

ALTER TABLE fct_node_active_last_24h ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Active nodes for the network';

ALTER TABLE fct_node_active_last_24h ON CLUSTER '{cluster}'
    COMMENT COLUMN `node_id` 'ID of the node';

ALTER TABLE fct_prepared_block_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Prepared block proposals showing what would have been built if the validator had been selected as proposer';

ALTER TABLE fct_prepared_block_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `meta_consensus_implementation` 'Consensus implementation of the validator';

ALTER TABLE fct_prepared_block_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `meta_consensus_version` 'Consensus version of the validator';

ALTER TABLE fct_prepared_block ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Prepared block proposals showing what would have been built if the validator had been selected as proposer';

ALTER TABLE fct_prepared_block ON CLUSTER '{cluster}'
    COMMENT COLUMN `meta_consensus_implementation` 'Consensus implementation of the validator';

ALTER TABLE fct_prepared_block ON CLUSTER '{cluster}'
    COMMENT COLUMN `meta_consensus_version` 'Consensus version of the validator';

ALTER TABLE fct_missed_slot_rate_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly missed slot rate with moving averages';

ALTER TABLE fct_missed_slot_rate_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly missed slot rate with moving averages';

ALTER TABLE fct_missed_slot_rate_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily missed slot rate with moving averages';

ALTER TABLE fct_missed_slot_rate_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily missed slot rate with moving averages';

ALTER TABLE fct_execution_tps_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly aggregated execution layer TPS statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_tps_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `avg_tps` 'Average TPS using actual block time gaps';

ALTER TABLE fct_execution_tps_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly aggregated execution layer TPS statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_tps_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `avg_tps` 'Average TPS using actual block time gaps';

ALTER TABLE fct_execution_tps_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily aggregated execution layer TPS statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_tps_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `avg_tps` 'Average TPS using actual block time gaps';

ALTER TABLE fct_execution_tps_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily aggregated execution layer TPS statistics with percentiles, Bollinger bands, and moving averages';

ALTER TABLE fct_execution_tps_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `avg_tps` 'Average TPS using actual block time gaps';
