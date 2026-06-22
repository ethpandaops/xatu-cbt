-- ============================================================================
-- Rollback: Drop all tables created in migration 086
-- ============================================================================
DROP TABLE IF EXISTS int_rocketpool_node_event ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_rocketpool_node_event_local ON CLUSTER '{cluster}';
