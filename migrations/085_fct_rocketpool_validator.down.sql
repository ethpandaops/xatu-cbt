-- ============================================================================
-- Rollback: Drop all tables created in migration 085
-- ============================================================================
-- Drop distributed tables first, then local tables.

DROP TABLE IF EXISTS fct_rocketpool_validator ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_rocketpool_validator_local ON CLUSTER '{cluster}';
