-- Rebuild the per-entity client counts from scratch: rows counted before entities were filled in
-- have an empty entity, and no later run writes that key again to replace them.
TRUNCATE TABLE IF EXISTS fct_block_graffiti_client_by_entity_hourly_local ON CLUSTER '{cluster}' SYNC;
TRUNCATE TABLE IF EXISTS fct_block_graffiti_client_by_entity_daily_local ON CLUSTER '{cluster}' SYNC;

ALTER TABLE admin_cbt_incremental_local ON CLUSTER '{cluster}'
DELETE WHERE `table` IN (
    'fct_block_graffiti_client_by_entity_hourly',
    'fct_block_graffiti_client_by_entity_daily'
) SETTINGS mutations_sync = 2;
