-- Drop unused projections from int_transaction_call_frame_opcode_gas_local
-- These projections add heavily to INSERT cost (and disk), and can we worked around.

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_frame;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_opcode;

-- Drop unused min_depth/max_depth columns from int_transaction_call_frame_opcode_gas

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS min_depth;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS max_depth;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS min_depth;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS max_depth;

-- Drop unused projections from int_transaction_opcode_gas_local

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_opcode;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_transaction;

-- Drop unused min_depth/max_depth columns from int_transaction_opcode_gas

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS min_depth;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS max_depth;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS min_depth;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS max_depth;
