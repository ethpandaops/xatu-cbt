-- ============================================================================
-- Rollback: Drop all tables created in migration 084
-- ============================================================================
-- Drop distributed tables first, then local tables.

DROP TABLE IF EXISTS int_rocketpool_minipool ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_rocketpool_minipool_local ON CLUSTER '{cluster}';
