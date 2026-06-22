-- ============================================================================
-- Rollback: Drop all tables created in migration 085
-- ============================================================================
DROP TABLE IF EXISTS int_rocketpool_megapool_validator ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_rocketpool_megapool_validator_local ON CLUSTER '{cluster}';
