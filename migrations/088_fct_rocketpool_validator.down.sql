-- ============================================================================
-- Rollback: Drop all tables created in migration 088
-- ============================================================================
DROP TABLE IF EXISTS fct_rocketpool_validator ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_rocketpool_validator_local ON CLUSTER '{cluster}';
