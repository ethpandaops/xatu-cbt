---
table: fct_block_head
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - proposer
  - head
dependencies:
  - "{{external}}.beacon_api_eth_v2_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMax(slot, updated_date_time) AS slot,
    slot_start_date_time,
    argMax(epoch, updated_date_time) AS epoch,
    argMax(epoch_start_date_time, updated_date_time) AS epoch_start_date_time,
    block_root,
    argMax(block_version, updated_date_time) AS block_version,
    argMax(block_total_bytes, updated_date_time) AS block_total_bytes,
    argMax(block_total_bytes_compressed, updated_date_time) AS block_total_bytes_compressed,
    argMax(parent_root, updated_date_time) AS parent_root,
    argMax(state_root, updated_date_time) AS state_root,
    argMax(proposer_index, updated_date_time) AS proposer_index,
    argMax(eth1_data_block_hash, updated_date_time) AS eth1_data_block_hash,
    argMax(eth1_data_deposit_root, updated_date_time) AS eth1_data_deposit_root,
    argMax(execution_payload_block_hash, updated_date_time) AS execution_payload_block_hash,
    argMax(execution_payload_block_number, updated_date_time) AS execution_payload_block_number,
    argMax(execution_payload_fee_recipient, updated_date_time) AS execution_payload_fee_recipient,
    argMax(execution_payload_base_fee_per_gas, updated_date_time) AS execution_payload_base_fee_per_gas,
    argMax(execution_payload_blob_gas_used, updated_date_time) AS execution_payload_blob_gas_used,
    argMax(execution_payload_excess_blob_gas, updated_date_time) AS execution_payload_excess_blob_gas,
    argMax(execution_payload_gas_limit, updated_date_time) AS execution_payload_gas_limit,
    argMax(execution_payload_gas_used, updated_date_time) AS execution_payload_gas_used,
    argMax(execution_payload_state_root, updated_date_time) AS execution_payload_state_root,
    argMax(execution_payload_parent_hash, updated_date_time) AS execution_payload_parent_hash,
    argMax(execution_payload_transactions_count, updated_date_time) AS execution_payload_transactions_count,
    argMax(execution_payload_transactions_total_bytes, updated_date_time) AS execution_payload_transactions_total_bytes,
    argMax(execution_payload_transactions_total_bytes_compressed, updated_date_time) AS execution_payload_transactions_total_bytes_compressed
FROM `{{ index .dep "{{external}}" "beacon_api_eth_v2_beacon_block" "database" }}`.`beacon_api_eth_v2_beacon_block` FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot_start_date_time, block_root
