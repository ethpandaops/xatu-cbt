-- ============================================================================
-- Rollback: Drop all tables created in migration 089
-- ============================================================================
DROP TABLE IF EXISTS dim_rocketpool_node ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS dim_rocketpool_node_local ON CLUSTER '{cluster}';
