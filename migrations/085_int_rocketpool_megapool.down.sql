-- ============================================================================
-- Rollback: Drop all tables created in migration 085
-- ============================================================================
DROP TABLE IF EXISTS int_rocketpool_megapool ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_rocketpool_megapool_local ON CLUSTER '{cluster}';
