DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_address_lifespan ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_address_lifespan_local ON CLUSTER '{cluster}' SYNC;

DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_address_activity_log ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_address_activity_log_local ON CLUSTER '{cluster}' SYNC;
