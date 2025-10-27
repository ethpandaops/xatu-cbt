---
table: fct_block
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - canonical
dependencies:
  - "{{transformation}}.int_block_canonical"
  - "{{transformation}}.fct_block_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH canonical_blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        block_version,
        block_total_bytes,
        block_total_bytes_compressed,
        parent_root,
        state_root,
        proposer_index,
        eth1_data_block_hash,
        eth1_data_deposit_root,
        execution_payload_block_hash,
        execution_payload_block_number,
        execution_payload_fee_recipient,
        execution_payload_base_fee_per_gas,
        execution_payload_blob_gas_used,
        execution_payload_excess_blob_gas,
        execution_payload_gas_limit,
        execution_payload_gas_used,
        execution_payload_state_root,
        execution_payload_parent_hash,
        execution_payload_transactions_count,
        execution_payload_transactions_total_bytes,
        execution_payload_transactions_total_bytes_compressed,
        'canonical' AS `status`
    FROM {{ index .dep "{{transformation}}" "int_block_canonical" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
orphaned_blocks AS (
    SELECT
        h.slot AS slot,
        h.slot_start_date_time AS slot_start_date_time,
        h.epoch AS epoch,
        h.epoch_start_date_time AS epoch_start_date_time,
        h.block_root AS block_root,
        h.block_version AS block_version,
        h.block_total_bytes AS block_total_bytes,
        h.block_total_bytes_compressed AS block_total_bytes_compressed,
        h.parent_root AS parent_root,
        h.state_root AS state_root,
        h.proposer_index AS proposer_index,
        h.eth1_data_block_hash AS eth1_data_block_hash,
        h.eth1_data_deposit_root AS eth1_data_deposit_root,
        h.execution_payload_block_hash AS execution_payload_block_hash,
        h.execution_payload_block_number AS execution_payload_block_number,
        h.execution_payload_fee_recipient AS execution_payload_fee_recipient,
        h.execution_payload_base_fee_per_gas AS execution_payload_base_fee_per_gas,
        h.execution_payload_blob_gas_used AS execution_payload_blob_gas_used,
        h.execution_payload_excess_blob_gas AS execution_payload_excess_blob_gas,
        h.execution_payload_gas_limit AS execution_payload_gas_limit,
        h.execution_payload_gas_used AS execution_payload_gas_used,
        h.execution_payload_state_root AS execution_payload_state_root,
        h.execution_payload_parent_hash AS execution_payload_parent_hash,
        h.execution_payload_transactions_count AS execution_payload_transactions_count,
        h.execution_payload_transactions_total_bytes AS execution_payload_transactions_total_bytes,
        h.execution_payload_transactions_total_bytes_compressed AS execution_payload_transactions_total_bytes_compressed,
        'orphaned' AS `status`
    FROM {{ index .dep "{{transformation}}" "fct_block_head" "helpers" "from" }} AS h FINAL
    GLOBAL LEFT ANTI JOIN canonical_blocks c 
        ON h.slot_start_date_time = c.slot_start_date_time 
        AND h.block_root = c.block_root
    WHERE h.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    *
FROM (
    SELECT * FROM canonical_blocks
    UNION ALL
    SELECT * FROM orphaned_blocks
)
