---
table: fct_block_mev_head
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - mev
  - head
dependencies:
  - "{{transformation}}.fct_block_head"
  - "{{external}}.mev_relay_bid_trace"
  - "{{external}}.mev_relay_proposer_payload_delivered"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        execution_payload_block_hash AS block_hash,
        execution_payload_parent_hash AS parent_hash,
        execution_payload_block_number AS block_number
    FROM {{ index .dep "{{transformation}}" "fct_block_head" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
proposer_payloads_raw AS (
  SELECT
    block_hash,
    builder_pubkey,
    proposer_pubkey,
    proposer_fee_recipient,
    gas_limit,
    gas_used,
    num_tx AS transaction_count,
    relay_name
  FROM {{ index .dep "{{external}}" "mev_relay_proposer_payload_delivered" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
),
bid_traces_raw AS (
  SELECT
    block_hash,
    min(timestamp_ms) AS min_timestamp_ms,
    max(value) AS max_value
  FROM {{ index .dep "{{external}}" "mev_relay_bid_trace" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
  GROUP BY block_hash
),
blocks_with_payloads AS (
  SELECT
    b.*,
    p.builder_pubkey,
    p.proposer_pubkey,
    p.proposer_fee_recipient,
    p.gas_limit,
    p.gas_used,
    p.transaction_count,
    p.relay_name
  FROM blocks b
  INNER JOIN proposer_payloads_raw p ON b.block_hash = p.block_hash
),
payload_aggregated AS (
  SELECT
    block_hash,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    parent_hash,
    block_number,
    any(builder_pubkey) AS builder_pubkey,
    any(proposer_pubkey) AS proposer_pubkey,
    any(proposer_fee_recipient) AS proposer_fee_recipient,
    any(gas_limit) AS gas_limit,
    any(gas_used) AS gas_used,
    any(transaction_count) AS transaction_count,
    groupArray(DISTINCT relay_name) AS relay_names
  FROM blocks_with_payloads
  GROUP BY block_hash, slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, parent_hash, block_number
)

SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  pa.slot,
  pa.slot_start_date_time,
  pa.epoch,
  pa.epoch_start_date_time,
  pa.block_root,
  CASE 
    WHEN bt.min_timestamp_ms != 0 
    THEN toDateTime64(bt.min_timestamp_ms / 1000, 3) 
    ELSE NULL 
  END AS earliest_bid_date_time,
  pa.relay_names,
  pa.parent_hash,
  pa.block_number,
  pa.block_hash,
  pa.builder_pubkey,
  pa.proposer_pubkey,
  pa.proposer_fee_recipient,
  pa.gas_limit,
  pa.gas_used,
  CASE
    WHEN bt.max_value != 0
    THEN bt.max_value
    ELSE NULL
  END AS value,
  pa.transaction_count
FROM payload_aggregated pa
LEFT JOIN bid_traces_raw bt ON pa.block_hash = bt.block_hash
