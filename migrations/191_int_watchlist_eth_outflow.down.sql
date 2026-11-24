-- ============================================================================
-- Rollback: Drop all tables created in migration 191
-- ============================================================================
DROP TABLE IF EXISTS int_watchlist_eth_outflow ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_watchlist_eth_outflow_local ON CLUSTER '{cluster}';
