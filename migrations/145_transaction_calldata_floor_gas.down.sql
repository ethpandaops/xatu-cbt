-- Restore the intrinsic_gas comment set in 056_gas_profiling.up.sql.

ALTER TABLE int_transaction_call_frame ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `calldata_floor_gas`;

ALTER TABLE int_transaction_call_frame ON CLUSTER '{cluster}'
    COMMENT COLUMN `intrinsic_gas` 'Intrinsic tx cost (21000 + calldata). Only populated for root frame of successful txs.';

ALTER TABLE int_transaction_call_frame_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `calldata_floor_gas`;

ALTER TABLE int_transaction_call_frame_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `intrinsic_gas` 'Intrinsic tx cost (21000 + calldata). Only populated for root frame of successful txs.';
