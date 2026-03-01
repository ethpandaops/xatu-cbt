-- Drop tables in reverse order (distributed then local)

DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_execution_receipt_size_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_execution_receipt_size_hourly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_execution_receipt_size_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_execution_receipt_size_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_block_receipt_size ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_block_receipt_size_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_receipt_size ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_receipt_size_local ON CLUSTER '{cluster}';
