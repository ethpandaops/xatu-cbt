---
table: fct_transaction_inclusion
type: incremental
interval:
  type: slot
  max: 21600
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - transaction
  - execution
  - canonical
dependencies:
  - "{{external}}.canonical_beacon_block_execution_transaction"
  - "{{transformation}}.fct_block"
---
-- One row per transaction included in a canonical beacon block execution payload.
-- Carries no mempool or relay context so its processable range is not gated by
-- mempool coverage and it can span the full post-Merge history.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH blocks AS (
    SELECT
        slot_start_date_time,
        block_root,
        execution_payload_block_number
    FROM {{ index .dep "{{transformation}}" "fct_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND status = 'canonical'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    tx.slot AS slot,
    tx.slot_start_date_time AS slot_start_date_time,
    tx.epoch AS epoch,
    tx.epoch_start_date_time AS epoch_start_date_time,
    tx.block_root AS block_root,
    tx.block_version AS block_version,
    CAST(b.execution_payload_block_number AS Nullable(UInt64)) AS block_number,
    tx.position AS position,
    tx.hash AS hash,
    tx.`from` AS `from`,
    tx.`to` AS `to`,
    tx.nonce AS nonce,
    tx.type AS type,
    tx.gas AS gas,
    tx.gas_price AS gas_price,
    tx.gas_tip_cap AS gas_tip_cap,
    tx.gas_fee_cap AS gas_fee_cap,
    tx.value AS value,
    tx.size AS size,
    tx.call_data_size AS call_data_size,
    tx.blob_gas AS blob_gas,
    tx.blob_gas_fee_cap AS blob_gas_fee_cap,
    tx.blob_hashes AS blob_hashes,
    ifNull(tx.`to` = tx.`from`, 0) AND (tx.value = 0) AS is_cancel_shape
FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_transaction" "helpers" "from" }} tx
GLOBAL LEFT JOIN blocks b
    ON b.slot_start_date_time = tx.slot_start_date_time AND b.block_root = tx.block_root
WHERE
    tx.meta_network_name = '{{ .env.NETWORK }}'
    AND tx.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
SETTINGS join_use_nulls = 1
