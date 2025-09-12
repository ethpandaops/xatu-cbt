---
table: fct_mev_relay_bid
interval:
  max: 500000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - canonical
dependencies:
  - "{{external}}.mev_relay_bid_trace"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    requested_at_slot_time,
    response_at_slot_time,
    toDateTime64(timestamp_ms / 1000, 3) as bid_date_time,
    relay_name,
    parent_hash,
    block_number,
    block_hash,
    builder_pubkey,
    proposer_pubkey,
    proposer_fee_recipient,
    gas_limit,
    gas_used,
    value,
    num_tx as transaction_count,
    optimistic_submission
FROM `{{ index .dep "{{external}}" "mev_relay_bid_trace" "database" }}`.`mev_relay_bid_trace` FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
