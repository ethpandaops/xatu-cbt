CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame_local ON CLUSTER '{cluster}' (
  -- Metadata
  `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

  -- Block and transaction identifiers
  `block_number` UInt64 COMMENT 'The block number containing this transaction' CODEC(DoubleDelta, ZSTD(1)),
  `transaction_hash` FixedString(66) COMMENT 'The transaction hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
  `transaction_index` UInt32 COMMENT 'Position of the transaction within the block' CODEC(DoubleDelta, ZSTD(1)),

  -- Call frame identifiers
  `call_frame_id` UInt32 COMMENT 'Sequential frame ID within the transaction (0 = root)' CODEC(DoubleDelta, ZSTD(1)),
  `parent_call_frame_id` Nullable(UInt32) COMMENT 'Parent frame ID (NULL for root frame)' CODEC(ZSTD(1)),
  `depth` UInt32 COMMENT 'Call depth (0 = root transaction execution)' CODEC(DoubleDelta, ZSTD(1)),

  -- Call information
  `target_address` Nullable(String) COMMENT 'Contract address being called (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
  `call_type` LowCardinality(String) COMMENT 'Type of call opcode (CALL, DELEGATECALL, STATICCALL, CALLCODE, CREATE, CREATE2)' CODEC(ZSTD(1)),
  `function_selector` Nullable(String) COMMENT 'Function selector (first 4 bytes of call input, hex encoded with 0x prefix). Populated for all frames from traces.' CODEC(ZSTD(1)),

  -- Aggregated metrics
  `opcode_count` UInt64 COMMENT 'Number of opcodes executed in this frame' CODEC(ZSTD(1)),
  `error_count` UInt64 COMMENT 'Number of opcodes that resulted in errors' CODEC(ZSTD(1)),

  -- Gas metrics (see transformation SQL for full gas model explanation)
  -- Receipt gas = (intrinsic + gas_cumulative) - min(gas_refund, (intrinsic + gas_cumulative) / 5)
  `gas` UInt64 COMMENT 'Gas consumed by this frame only, excludes child frames. sum(gas) = EVM execution gas. This is "self" gas in flame graphs.' CODEC(ZSTD(1)),
  `gas_cumulative` UInt64 COMMENT 'Gas consumed by this frame + all descendants. Root frame value = total EVM execution gas.' CODEC(ZSTD(1)),
  `gas_refund` Nullable(UInt64) COMMENT 'Total accumulated refund. Only populated for root frame (refund applied once at tx end).' CODEC(ZSTD(1)),
  `intrinsic_gas` Nullable(UInt64) COMMENT 'Intrinsic tx cost (21000 + calldata). Only populated for root frame (call_frame_id=0).' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
  '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
  '{replica}',
  updated_date_time
)
PARTITION BY intDiv(block_number, 201600) -- ~1 month of blocks
ORDER BY (block_number, transaction_hash, call_frame_id)
SETTINGS
  deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Aggregated call frame activity per transaction for call tree analysis';

CREATE TABLE `${NETWORK_NAME}`.int_transaction_call_frame ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_transaction_call_frame_local
ENGINE = Distributed(
  '{cluster}',
  '${NETWORK_NAME}',
  int_transaction_call_frame_local,
  cityHash64(block_number, transaction_hash)
);

-- Projection for transaction lookups without block_number
ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_transaction (
  SELECT *
  ORDER BY (transaction_hash, call_frame_id)
);
