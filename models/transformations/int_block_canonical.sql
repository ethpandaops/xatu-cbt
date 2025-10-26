---
table: int_block_canonical
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
  - proposer
  - canonical
dependencies:
  - "{{external}}.canonical_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
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
    execution_payload_transactions_total_bytes_compressed
FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
