---
table: fct_payload_bid_highest_value_by_builder_chunked_50ms
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - payload
  - bid
  - epbs
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_execution_payload_bid"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas (ePBS): the on-chain bid race, mirroring the relay-era
-- fct_mev_bid_highest_value_by_builder_chunked_50ms but sourced from builder
-- bids on gossip. Multiple sentries observe the same bid; the earliest
-- observation wins. Bids can arrive before the slot starts, so
-- chunk_slot_start_diff is negative for pre-slot bids. Bid amounts are Gwei
-- on the wire; stored as wei to match the mev_relay bid tables.
WITH bids AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_index,
      block_hash,
      toUInt128(`value`) * 1000000000 AS `value`,
      toUInt128(execution_payment) * 1000000000 AS execution_payment,
      fee_recipient,
      toInt64(propagation_slot_start_diff) AS bid_slot_start_diff,
      toUnixTimestamp64Milli(event_date_time) AS event_ms
  FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload_bid" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND propagation_slot_start_diff >= -12000
    AND propagation_slot_start_diff < 12000
),
earliest_bid_per_block AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_index,
      block_hash,
      `value`,
      min(bid_slot_start_diff) AS earliest_slot_start_diff,
      argMin(execution_payment, bid_slot_start_diff) AS execution_payment,
      argMin(fee_recipient, bid_slot_start_diff) AS fee_recipient,
      argMin(event_ms, bid_slot_start_diff) AS earliest_event_ms
  FROM bids
  GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, builder_index, block_hash, `value`
),
bids_with_chunks AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_index,
      block_hash,
      floor(earliest_slot_start_diff / 50) * 50 AS chunk_slot_start_diff,
      `value`,
      execution_payment,
      fee_recipient,
      earliest_event_ms
  FROM earliest_bid_per_block
),
chunk_max AS (
  SELECT
      slot,
      slot_start_date_time,
      epoch,
      epoch_start_date_time,
      builder_index,
      chunk_slot_start_diff,
      max(`value`) AS max_value,
      argMax(earliest_event_ms, `value`) AS earliest_event_ms,
      argMax(block_hash, `value`) AS block_hash,
      argMax(execution_payment, `value`) AS execution_payment,
      argMax(fee_recipient, `value`) AS fee_recipient
  FROM bids_with_chunks
  GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, builder_index, chunk_slot_start_diff
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    toInt32(chunk_slot_start_diff) AS chunk_slot_start_diff,
    toDateTime64(earliest_event_ms / 1000, 3) AS earliest_bid_date_time,
    block_hash,
    builder_index,
    max_value AS `value`,
    execution_payment,
    fee_recipient
FROM chunk_max
