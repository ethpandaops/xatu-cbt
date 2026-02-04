-- Re-add projections to int_transaction_call_frame_opcode_gas_local

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_frame (
    SELECT *
    ORDER BY (transaction_hash, call_frame_id, opcode)
);

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number, transaction_hash, call_frame_id)
);

-- Re-add projections to int_transaction_opcode_gas_local

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_opcode (
    SELECT *
    ORDER BY (opcode, block_number, transaction_hash)
);

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_transaction (
    SELECT *
    ORDER BY (transaction_hash, opcode)
);
