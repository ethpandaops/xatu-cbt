ALTER TABLE fct_data_column_availability_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by UTC day and column index, rolled up from fct_data_column_availability_hourly: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by UTC day and column index, rolled up from fct_data_column_availability_hourly: almost all observations are gossipsub sightings, which always count as a success';

DROP TABLE IF EXISTS fct_opcode_cost_by_el_client_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_opcode_cost_by_el_client_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_prepared_block_by_node_daily ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_prepared_block_by_node_daily_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_opcode_gas_by_opcode_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_opcode_gas_by_opcode_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_graffiti_client_by_entity_group_daily ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_graffiti_client_by_entity_group_daily_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_graffiti_client_version_by_entity_group_daily ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_graffiti_client_version_by_entity_group_daily_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_graffiti_client_by_entity_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_graffiti_client_by_entity_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_graffiti_client_version_by_entity_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_graffiti_client_version_by_entity_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_data_column_sidecar_first_seen_by_node_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_data_column_sidecar_first_seen_by_node_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_attestation_observation_by_node_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_attestation_observation_by_node_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_head_first_seen_by_node_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_head_first_seen_by_node_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_first_seen_by_node_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_first_seen_by_node_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_first_seen_by_network_weekly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_first_seen_by_network_weekly_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_first_seen_by_network_daily ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_first_seen_by_network_daily_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_first_seen_by_network_hourly ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_block_first_seen_by_network_hourly_local ON CLUSTER '{cluster}' SYNC;
