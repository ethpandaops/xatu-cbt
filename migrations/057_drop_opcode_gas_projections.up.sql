-- Drop unused projections from int_transaction_call_frame_opcode_gas_local
-- These projections tripled INSERT cost (data written 3x with different sort orders)
-- but are not used by any downstream transformations.
-- p_by_frame: used by frontend queries, can be re-added if needed
-- p_by_opcode: not used by any current query

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_frame;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_opcode;

-- Drop unused projections from int_transaction_opcode_gas_local

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_opcode;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_transaction;
