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

-- Re-add min_depth/max_depth columns to int_transaction_call_frame_opcode_gas

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    ADD COLUMN `min_depth` UInt64 COMMENT 'Minimum call stack depth for this opcode in this frame' CODEC(ZSTD(1)) AFTER `gas_cumulative`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    ADD COLUMN `max_depth` UInt64 COMMENT 'Maximum call stack depth for this opcode in this frame' CODEC(ZSTD(1)) AFTER `min_depth`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
    ADD COLUMN `min_depth` UInt64 COMMENT 'Minimum call stack depth for this opcode in this frame' CODEC(ZSTD(1)) AFTER `gas_cumulative`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
    ADD COLUMN `max_depth` UInt64 COMMENT 'Maximum call stack depth for this opcode in this frame' CODEC(ZSTD(1)) AFTER `min_depth`;

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

-- Re-add min_depth/max_depth columns to int_transaction_opcode_gas

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    ADD COLUMN `min_depth` UInt64 COMMENT 'Minimum call stack depth for this opcode' CODEC(ZSTD(1)) AFTER `gas_cumulative`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas_local ON CLUSTER '{cluster}'
    ADD COLUMN `max_depth` UInt64 COMMENT 'Maximum call stack depth for this opcode' CODEC(ZSTD(1)) AFTER `min_depth`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas ON CLUSTER '{cluster}'
    ADD COLUMN `min_depth` UInt64 COMMENT 'Minimum call stack depth for this opcode' CODEC(ZSTD(1)) AFTER `gas_cumulative`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_opcode_gas ON CLUSTER '{cluster}'
    ADD COLUMN `max_depth` UInt64 COMMENT 'Maximum call stack depth for this opcode' CODEC(ZSTD(1)) AFTER `min_depth`;
