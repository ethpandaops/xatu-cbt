---
table: fct_payload_bid_highest_value_chunked_50ms
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
-- Gloas (ePBS): the auction frontier — the single best bid per 50ms chunk
-- across ALL builders, with the builder that led it. Row count per slot is
-- bounded by the chunk grid no matter how many builders compete, so this is
-- the scalable series for charts, while the by_builder variant remains for
-- drill-down. Bid amounts are Gwei on the wire and stored as wei to match
-- the mev_relay bid tables.
WITH bids AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_index,
      block_hash,
      toUInt128(`value`) * 1000000000 AS `value`,
      toInt64(propagation_slot_start_diff) AS bid_slot_start_diff,
      toUnixTimestamp64Milli(event_date_time) AS event_ms
  FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload_bid" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND propagation_slot_start_diff >= -12000
    AND propagation_slot_start_diff < 12000
),
earliest_observation_per_bid AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_index,
      block_hash,
      `value`,
      min(bid_slot_start_diff) AS earliest_slot_start_diff,
      argMin(event_ms, bid_slot_start_diff) AS earliest_event_ms
  FROM bids
  GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, builder_index, block_hash, `value`
),
chunked AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_index,
      block_hash,
      `value`,
      floor(earliest_slot_start_diff / 50) * 50 AS chunk_slot_start_diff,
      earliest_event_ms
  FROM earliest_observation_per_bid
),
chunk_best AS (
  SELECT
      slot,
      slot_start_date_time,
      epoch,
      epoch_start_date_time,
      chunk_slot_start_diff,
      max(`value`) AS max_value,
      argMax(earliest_event_ms, `value`) AS earliest_event_ms,
      argMax(block_hash, `value`) AS block_hash,
      argMax(builder_index, `value`) AS builder_index
  FROM chunked
  GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, chunk_slot_start_diff
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
    max_value AS `value`
FROM chunk_best
