-- ============================================================================
-- Rollback: Drop all tables created in migration 087
-- ============================================================================
DROP TABLE IF EXISTS int_rocketpool_node_timezone ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_rocketpool_node_timezone_local ON CLUSTER '{cluster}';
