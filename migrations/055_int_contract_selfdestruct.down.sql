-- ============================================================================
-- Migration: Drop Contract Creation, SELFDESTRUCT, and Storage Diffs tables
-- ============================================================================
-- Drop in reverse order of creation

DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_selfdestruct_diffs ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_selfdestruct_diffs_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_selfdestruct ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_selfdestruct_local ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_creation ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_contract_creation_local ON CLUSTER '{cluster}';
