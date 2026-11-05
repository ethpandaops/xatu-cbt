-- int_transaction_call_frame stores the EIP-7623 calldata floor on the root frame, and
-- intrinsic_gas comes from structlog_agg, which computes it from the transaction.
--
-- Columns live on both the _local and the Distributed table, so both are updated.

ALTER TABLE int_transaction_call_frame_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `calldata_floor_gas` Nullable(UInt64) COMMENT 'EIP-7623 calldata floor, 21000 + 10 * (zero input bytes + 4 * non-zero input bytes). The receipt is max(intrinsic_gas + gas_cumulative - min(gas_refund, (intrinsic_gas + gas_cumulative) / 5), calldata_floor_gas). Only populated for root frame (call_frame_id=0) from Prague, NULL before' CODEC(ZSTD(1));

ALTER TABLE int_transaction_call_frame_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `intrinsic_gas` 'Intrinsic tx cost from the transaction: 21000 (53000 for a creation) + calldata + initcode words + access list + EIP-7702 authorizations. Excludes refunds and the calldata floor. Only populated for root frame (call_frame_id=0).';

ALTER TABLE int_transaction_call_frame ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `calldata_floor_gas` Nullable(UInt64) COMMENT 'EIP-7623 calldata floor, 21000 + 10 * (zero input bytes + 4 * non-zero input bytes). The receipt is max(intrinsic_gas + gas_cumulative - min(gas_refund, (intrinsic_gas + gas_cumulative) / 5), calldata_floor_gas). Only populated for root frame (call_frame_id=0) from Prague, NULL before';

ALTER TABLE int_transaction_call_frame ON CLUSTER '{cluster}'
    COMMENT COLUMN `intrinsic_gas` 'Intrinsic tx cost from the transaction: 21000 (53000 for a creation) + calldata + initcode words + access list + EIP-7702 authorizations. Excludes refunds and the calldata floor. Only populated for root frame (call_frame_id=0).';
