-- Remove row_count column from fct_block_data_column_sidecar_first_seen_by_node table

-- Remove from distributed table first
ALTER TABLE `${NETWORK_NAME}`.fct_block_data_column_sidecar_first_seen_by_node ON CLUSTER '{cluster}'
DROP COLUMN IF EXISTS `row_count`;

-- Remove from local table
ALTER TABLE `${NETWORK_NAME}`.fct_block_data_column_sidecar_first_seen_by_node_local ON CLUSTER '{cluster}'
DROP COLUMN IF EXISTS `row_count`;
