-- ============================================================================
-- Rollback: Drop all tables created in migration 086
-- ============================================================================
-- Drop distributed tables first, then local tables.

DROP TABLE IF EXISTS dim_rocketpool_node ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS dim_rocketpool_node_local ON CLUSTER '{cluster}';
