-- Drop new tables first (distributed then local, reverse order of creation)

DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_block_resource_gas ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_block_resource_gas_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_resource_gas ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_resource_gas_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_call_frame_opcode_resource_gas ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.int_transaction_call_frame_opcode_resource_gas_local ON CLUSTER '{cluster}';

-- Drop added columns

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `memory_words_sum_before`,
    DROP COLUMN IF EXISTS `memory_words_sum_after`,
    DROP COLUMN IF EXISTS `memory_words_sq_sum_before`,
    DROP COLUMN IF EXISTS `memory_words_sq_sum_after`,
    DROP COLUMN IF EXISTS `memory_expansion_gas`,
    DROP COLUMN IF EXISTS `cold_access_count`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `memory_words_sum_before`,
    DROP COLUMN IF EXISTS `memory_words_sum_after`,
    DROP COLUMN IF EXISTS `memory_words_sq_sum_before`,
    DROP COLUMN IF EXISTS `memory_words_sq_sum_after`,
    DROP COLUMN IF EXISTS `memory_expansion_gas`,
    DROP COLUMN IF EXISTS `cold_access_count`;
