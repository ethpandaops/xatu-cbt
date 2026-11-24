DROP TABLE IF EXISTS fct_block_graffiti_client_by_entity_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_block_graffiti_client_by_entity_daily_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_block_graffiti_client_by_entity_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_block_graffiti_client_by_entity_hourly_local ON CLUSTER '{cluster}';

ALTER TABLE fct_block_graffiti ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `entity`;

ALTER TABLE fct_block_graffiti_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `entity`;
