-- Drop in reverse order of creation

-- fct_block_opcode_gas
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_block_opcode_gas ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_block_opcode_gas_local ON CLUSTER '{cluster}';

-- dim_function_signature
DROP TABLE IF EXISTS `${NETWORK_NAME}`.dim_function_signature ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS `${NETWORK_NAME}`.dim_function_signature_local ON CLUSTER '{cluster}' SYNC;

-- int_transaction_call_frame_opcode_gas
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}';

-- int_transaction_call_frame
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_call_frame ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_call_frame_local ON CLUSTER '{cluster}';

-- int_transaction_opcode_gas
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_opcode_gas ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}';
