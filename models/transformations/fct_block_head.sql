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
  # Gloas payload-hash source. OR-grouped with the beacon block table so
  # networks where the execution_payload events table is empty or absent
  # schedule unaffected.
  - - "{{external}}.beacon_api_eth_v1_events_execution_payload"
    - "{{external}}.beacon_api_eth_v2_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Gloas (ePBS): the beacon block no longer embeds the execution payload, so
-- its execution_payload_block_hash is empty in the block table. The
-- execution_payload SSE events map each revealed payload's block hash to its
-- beacon block root. Empty on pre-gloas networks, where it contributes nothing.
payload_events AS (
    SELECT
        block_root,
        any(block_hash) AS payload_block_hash
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload" "helpers" "from" }}
    WHERE meta_network_name = '{{ .env.NETWORK }}'
        AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND block_hash != ''
    GROUP BY block_root
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMax(slot, updated_date_time) AS slot,
    slot_start_date_time,
    argMax(epoch, updated_date_time) AS epoch,
    argMax(epoch_start_date_time, updated_date_time) AS epoch_start_date_time,
    bb.block_root AS block_root,
    argMax(block_version, updated_date_time) AS block_version,
    argMax(block_total_bytes, updated_date_time) AS block_total_bytes,
    argMax(block_total_bytes_compressed, updated_date_time) AS block_total_bytes_compressed,
    argMax(parent_root, updated_date_time) AS parent_root,
    argMax(state_root, updated_date_time) AS state_root,
    argMax(proposer_index, updated_date_time) AS proposer_index,
    argMax(eth1_data_block_hash, updated_date_time) AS eth1_data_block_hash,
    argMax(eth1_data_deposit_root, updated_date_time) AS eth1_data_deposit_root,
    -- Pre-gloas blocks carry their payload hash. Gloas blocks fall back to
    -- the payload observed for their block root on the SSE layer
    coalesce(nullif(argMax(execution_payload_block_hash, updated_date_time), ''), any(pe.payload_block_hash), '') AS execution_payload_block_hash,
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
FROM {{ index .dep "{{external}}" "beacon_api_eth_v2_beacon_block" "helpers" "from" }} AS bb FINAL
GLOBAL LEFT JOIN payload_events pe ON bb.block_root = pe.block_root
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, block_root
