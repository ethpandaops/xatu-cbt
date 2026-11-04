ALTER TABLE fct_attestation_correctness_by_validator_head_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Attestation correctness by validator for the unfinalized chain. Forks in the chain may cause multiple block roots for the same slot to be present';

ALTER TABLE fct_attestation_correctness_by_validator_head ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Attestation correctness by validator for the unfinalized chain. Forks in the chain may cause multiple block roots for the same slot to be present';

ALTER TABLE int_attestation_attested_canonical_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Attested head of a block for the finalized chain.';

ALTER TABLE int_attestation_attested_canonical ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Attested head of a block for the finalized chain.';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index, from custody probe results and gossipsub data column sidecar sightings. A gossipsub sighting always counts as a success. A custody probe only counts if it was recorded before the slot was processed, and probes usually arrive hours later, so almost all rows are gossipsub sightings';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index, from custody probe results and gossipsub data column sidecar sightings. A gossipsub sighting always counts as a success. A custody probe only counts if it was recorded before the slot was processed, and probes usually arrive hours later, so almost all rows are gossipsub sightings';

ALTER TABLE fct_data_column_availability_by_slot_blob_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index, repeated once per blob index of the slot. Probes and sightings are per column, so every blob of a slot carries the same counts. Sources as fct_data_column_availability_by_slot: almost all rows are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_by_slot_blob ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index, repeated once per blob index of the slot. Probes and sightings are per column, so every blob of a slot carries the same counts. Sources as fct_data_column_availability_by_slot: almost all rows are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_by_epoch_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by epoch and column index, rolled up from fct_data_column_availability_by_slot: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_by_epoch ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by epoch and column index, rolled up from fct_data_column_availability_by_slot: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by hour and column index, rolled up from fct_data_column_availability_by_epoch: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by hour and column index, rolled up from fct_data_column_availability_by_epoch: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by UTC day and column index, rolled up from fct_data_column_availability_hourly: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by UTC day and column index, rolled up from fct_data_column_availability_hourly: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE int_custody_probe_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results with aggregated column indices, ordered by probe time. A probe usually runs hours after the slot it asks about';

ALTER TABLE int_custody_probe ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results with aggregated column indices, ordered by probe time. A probe usually runs hours after the slot it asks about';

ALTER TABLE int_custody_probe_order_by_slot_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results with aggregated column indices, ordered by the probed slot time. A probe usually runs hours after the slot it asks about';

ALTER TABLE int_custody_probe_order_by_slot ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results with aggregated column indices, ordered by the probed slot time. A probe usually runs hours after the slot it asks about';

ALTER TABLE fct_node_active_last_24h_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Xatu nodes that sent beacon API block events in the last 24 hours, one row per meta_client_name with its latest client, version and location. Nodes that only report through libp2p, such as the ethPandaOps libp2p sentries, are not included';

ALTER TABLE fct_node_active_last_24h_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `node_id` 'Hashed node segment of meta_client_name. It changes when the node restarts under a new name, so one machine can have several node_ids over time';

ALTER TABLE fct_node_active_last_24h ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Xatu nodes that sent beacon API block events in the last 24 hours, one row per meta_client_name with its latest client, version and location. Nodes that only report through libp2p, such as the ethPandaOps libp2p sentries, are not included';

ALTER TABLE fct_node_active_last_24h ON CLUSTER '{cluster}'
    COMMENT COLUMN `node_id` 'Hashed node segment of meta_client_name. It changes when the node restarts under a new name, so one machine can have several node_ids over time';

ALTER TABLE fct_prepared_block_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blocks that Xatu nodes built locally each slot by asking their beacon node for a block proposal, showing what that node would have proposed';

ALTER TABLE fct_prepared_block_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `meta_consensus_implementation` 'Consensus client implementation of the node that built the block';

ALTER TABLE fct_prepared_block_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `meta_consensus_version` 'Consensus client version of the node that built the block';

ALTER TABLE fct_prepared_block ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blocks that Xatu nodes built locally each slot by asking their beacon node for a block proposal, showing what that node would have proposed';

ALTER TABLE fct_prepared_block ON CLUSTER '{cluster}'
    COMMENT COLUMN `meta_consensus_implementation` 'Consensus client implementation of the node that built the block';

ALTER TABLE fct_prepared_block ON CLUSTER '{cluster}'
    COMMENT COLUMN `meta_consensus_version` 'Consensus client version of the node that built the block';

ALTER TABLE fct_missed_slot_rate_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly rate of slots without a canonical block. Missed slots and slots whose block was orphaned both count as missed';

ALTER TABLE fct_missed_slot_rate_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly rate of slots without a canonical block. Missed slots and slots whose block was orphaned both count as missed';

ALTER TABLE fct_missed_slot_rate_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily rate of slots without a canonical block. Missed slots and slots whose block was orphaned both count as missed';

ALTER TABLE fct_missed_slot_rate_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily rate of slots without a canonical block. Missed slots and slots whose block was orphaned both count as missed';

ALTER TABLE fct_execution_tps_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly execution layer TPS statistics. Per-block TPS is the block''s transaction count divided by the seconds since the previous block; avg_tps, the percentiles and the bands are taken over blocks, and total_transactions / total_seconds is the TPS over the hour';

ALTER TABLE fct_execution_tps_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `avg_tps` 'Unweighted mean of per-block TPS over the hour''s blocks. Short block gaps inflate it; total_transactions / total_seconds is the TPS over the hour';

ALTER TABLE fct_execution_tps_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly execution layer TPS statistics. Per-block TPS is the block''s transaction count divided by the seconds since the previous block; avg_tps, the percentiles and the bands are taken over blocks, and total_transactions / total_seconds is the TPS over the hour';

ALTER TABLE fct_execution_tps_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `avg_tps` 'Unweighted mean of per-block TPS over the hour''s blocks. Short block gaps inflate it; total_transactions / total_seconds is the TPS over the hour';

ALTER TABLE fct_execution_tps_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution layer TPS statistics. Per-block TPS is the block''s transaction count divided by the seconds since the previous block; avg_tps, the percentiles and the bands are taken over blocks, and total_transactions / total_seconds is the TPS over the UTC day';

ALTER TABLE fct_execution_tps_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `avg_tps` 'Unweighted mean of per-block TPS over the day''s blocks. Short block gaps inflate it; total_transactions / total_seconds is the TPS over the day';

ALTER TABLE fct_execution_tps_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution layer TPS statistics. Per-block TPS is the block''s transaction count divided by the seconds since the previous block; avg_tps, the percentiles and the bands are taken over blocks, and total_transactions / total_seconds is the TPS over the UTC day';

ALTER TABLE fct_execution_tps_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `avg_tps` 'Unweighted mean of per-block TPS over the day''s blocks. Short block gaps inflate it; total_transactions / total_seconds is the TPS over the day';
