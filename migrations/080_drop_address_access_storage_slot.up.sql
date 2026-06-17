-- ============================================================================
-- Migration 080: Drop address access + storage slot tables
-- ============================================================================
-- Removes the stateless address-access and storage-slot analysis tables.
-- Their CBT transformation models and tests have been removed.
--   - fct_address_* tables originally created in migration 021 (stateless)
--   - int_address_* tables originally created in migration 003 (execution_accounts)
-- ============================================================================

-- Drop distributed tables first, then their local backing tables.

-- fct_address_* (from migration 021)
DROP TABLE IF EXISTS fct_address_storage_slot_top_100_by_contract ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_storage_slot_top_100_by_contract_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_storage_slot_expired_top_100_by_contract ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_storage_slot_expired_top_100_by_contract_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_access_total ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_access_total_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_storage_slot_total ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_storage_slot_total_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_access_chunked_10000 ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_access_chunked_10000_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_storage_slot_chunked_10000 ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_address_storage_slot_chunked_10000_local ON CLUSTER '{cluster}';

-- int_address_* (from migration 003)
DROP TABLE IF EXISTS int_address_last_access ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_address_last_access_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_address_first_access ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_address_first_access_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_address_storage_slot_last_access ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_address_storage_slot_last_access_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_address_storage_slot_first_access ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS int_address_storage_slot_first_access_local ON CLUSTER '{cluster}';
