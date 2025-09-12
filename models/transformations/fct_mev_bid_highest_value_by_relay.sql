---
table: fct_mev_bid_highest_value_by_relay
interval:
  max: 5000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - mev
  - bid
dependencies:
  - "{{external}}.mev_relay_bid_trace"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH max_value AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      relay_name,
      argMax(toDateTime64(timestamp_ms / 1000, 3), value) AS bid_date_time,
      argMax(parent_hash, value) AS parent_hash,
      argMax(block_number, value) AS block_number,
      argMax(block_hash, value) AS block_hash,
      argMax(builder_pubkey, value) AS builder_pubkey,
      argMax(proposer_pubkey, value) AS proposer_pubkey,
      argMax(proposer_fee_recipient, value) AS proposer_fee_recipient,
      argMax(gas_limit, value) AS gas_limit,
      argMax(gas_used, value) AS gas_used,
      max(value) AS transaction_value,
      argMax(num_tx, value) AS transaction_count,
      argMax(timestamp, value) AS timestamp,
      argMax(optimistic_submission, value) AS optimistic_submission
  FROM `{{ index .dep "{{external}}" "mev_relay_bid_trace" "database" }}`.`mev_relay_bid_trace` FINAL
  GROUP BY slot_start_date_time, slot, epoch,  epoch_start_date_time,  relay_name
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    bid_date_time,
    relay_name,
    parent_hash,
    block_number,
    block_hash,
    builder_pubkey,
    proposer_pubkey,
    proposer_fee_recipient,
    gas_limit,
    gas_used,
    transaction_value AS value,
    transaction_count,
    optimistic_submission
FROM max_value
