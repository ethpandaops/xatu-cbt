ALTER TABLE fct_block_data_column_sidecar_first_seen ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `completed_via`;

ALTER TABLE fct_block_data_column_sidecar_first_seen_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `completed_via`;

ALTER TABLE fct_block_data_column_sidecar_first_seen_by_node ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `completed_via`;

ALTER TABLE fct_block_data_column_sidecar_first_seen_by_node_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `completed_via`;
