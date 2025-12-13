-- int_storage_slot_diff_latest_state
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_diff_latest_state ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_diff_latest_state_local ON CLUSTER '{cluster}';

-- int_storage_slot_latest_state
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_latest_state ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_latest_state_local ON CLUSTER '{cluster}';

-- int_storage_slot_reactivation_by_6m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_by_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_reactivation_by_6m_local ON CLUSTER '{cluster}';

-- int_storage_slot_read
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_read ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_read_local ON CLUSTER '{cluster}';

-- int_storage_slot_next_touch
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_next_touch ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_next_touch_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state_with_expiry_by_6m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_with_expiry_by_6m_local ON CLUSTER '{cluster}';

-- int_storage_slot_expiry_by_6m
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_by_6m ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_expiry_by_6m_local ON CLUSTER '{cluster}';

-- fct_storage_slot_state
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_storage_slot_state_local ON CLUSTER '{cluster}';

-- int_storage_slot_diff_by_address_slot
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_diff_by_address_slot ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_diff_by_address_slot_local ON CLUSTER '{cluster}';

-- int_storage_slot_diff
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_diff ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_storage_slot_diff_local ON CLUSTER '{cluster}';
