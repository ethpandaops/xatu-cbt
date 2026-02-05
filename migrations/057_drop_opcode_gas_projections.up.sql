-- Drop unused projections and min_depth/max_depth columns from int_transaction_call_frame_opcode_gas
-- Projections add heavily to INSERT cost (and disk), and can be worked around.
-- min_depth/max_depth are not consumed by any downstream transformation or frontend query.

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_frame,
    DROP PROJECTION IF EXISTS p_by_opcode,
    DROP COLUMN IF EXISTS min_depth,
    DROP COLUMN IF EXISTS max_depth;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS min_depth,
    DROP COLUMN IF EXISTS max_depth;

-- Drop unused projections and min_depth/max_depth columns from int_transaction_opcode_gas

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    DROP PROJECTION IF EXISTS p_by_opcode,
    DROP PROJECTION IF EXISTS p_by_transaction,
    DROP COLUMN IF EXISTS min_depth,
    DROP COLUMN IF EXISTS max_depth;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS min_depth,
    DROP COLUMN IF EXISTS max_depth;
