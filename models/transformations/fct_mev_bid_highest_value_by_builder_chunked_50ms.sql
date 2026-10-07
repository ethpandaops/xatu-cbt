---
table: fct_mev_bid_highest_value_by_builder_chunked_50ms
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - mev
  - bid
dependencies:
  # Relay bid traces, or from Gloas (ePBS) builder bids on gossip with the
  # builder pubkey from the builder registry.
  - [
    "{{external}}.mev_relay_bid_trace",
    "{{external}}.beacon_api_eth_v1_events_execution_payload_bid"
  ]
  - [
    "{{external}}.mev_relay_bid_trace",
    "{{external}}.canonical_beacon_state_builder"
  ]
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas bids are observed on gossip rather than at a relay: the earliest
-- sentry observation stands in for the relay timestamp, relay_names is empty,
-- and value is the bid value plus its execution payment, Gwei on the wire.
WITH bids AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_pubkey,
      block_hash,
      value,
      timestamp_ms,
      relay_name,
      toInt64(timestamp_ms - (toUnixTimestamp(slot_start_date_time) * 1000)) AS bid_slot_start_diff
  FROM {{ index .dep "{{external}}" "mev_relay_bid_trace" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND bid_slot_start_diff >= -12000
    AND bid_slot_start_diff < 12000
),
earliest_bid_per_block AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_pubkey,
      block_hash,
      min(timestamp_ms) AS earliest_timestamp_ms,
      argMin(value, timestamp_ms) AS value
  FROM bids
  GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, builder_pubkey, block_hash
),
bids_with_chunks AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_pubkey,
      block_hash,
      floor(toInt64(earliest_timestamp_ms - (toUnixTimestamp(slot_start_date_time) * 1000)) / 50) * 50 AS chunk_slot_start_diff,
      value,
      earliest_timestamp_ms
  FROM earliest_bid_per_block
),
max_value_per_chunk AS (
  SELECT
      slot,
      builder_pubkey,
      chunk_slot_start_diff,
      max(value) AS max_value
  FROM bids_with_chunks
  GROUP BY slot, builder_pubkey, chunk_slot_start_diff
),
bids_chunked AS (
  SELECT
      bc.slot_start_date_time,
      bc.slot,
      bc.epoch,
      bc.epoch_start_date_time,
      bc.builder_pubkey,
      bc.block_hash,
      bc.chunk_slot_start_diff,
      bc.value AS max_value,
      toDateTime64(bc.earliest_timestamp_ms / 1000, 3) AS earliest_bid_date_time
  FROM bids_with_chunks AS bc
  GLOBAL INNER JOIN max_value_per_chunk AS mvc
    ON bc.slot = mvc.slot
    AND bc.builder_pubkey = mvc.builder_pubkey
    AND bc.chunk_slot_start_diff = mvc.chunk_slot_start_diff
    AND bc.value = mvc.max_value
),
relay_aggregation AS (
  SELECT
      bc.slot,
      bc.builder_pubkey,
      bc.block_hash,
      groupArray(DISTINCT b.relay_name) AS relay_names
  FROM bids_chunked AS bc
  GLOBAL INNER JOIN bids AS b
    ON bc.slot = b.slot
    AND bc.builder_pubkey = b.builder_pubkey
    AND bc.block_hash = b.block_hash
  GROUP BY bc.slot, bc.builder_pubkey, bc.block_hash
),
max_bid_details AS (
  SELECT
      bc.slot_start_date_time,
      bc.slot,
      bc.epoch,
      bc.epoch_start_date_time,
      bc.builder_pubkey,
      bc.block_hash,
      bc.chunk_slot_start_diff,
      bc.max_value AS transaction_value,
      bc.earliest_bid_date_time,
      ra.relay_names
  FROM bids_chunked AS bc
  GLOBAL INNER JOIN relay_aggregation AS ra
    ON bc.slot = ra.slot
    AND bc.builder_pubkey = ra.builder_pubkey
    AND bc.block_hash = ra.block_hash
),
gossip_bids AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_index,
      block_hash,
      (toUInt128(any(`value`)) + toUInt128(any(execution_payment))) * 1000000000 AS bid_value,
      min(toInt64(propagation_slot_start_diff)) AS earliest_slot_start_diff,
      min(event_date_time) AS earliest_bid_date_time
  FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload_bid" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND builder_index IS NOT NULL
    AND propagation_slot_start_diff >= -12000
    AND propagation_slot_start_diff < 12000
  GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, builder_index, block_hash
),
gossip_chunk_max AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_index,
      floor(earliest_slot_start_diff / 50) * 50 AS chunk_slot_start_diff,
      max(bid_value) AS max_value,
      argMax(block_hash, bid_value) AS max_block_hash,
      argMax(earliest_bid_date_time, bid_value) AS max_earliest_bid_date_time
  FROM gossip_bids
  GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, builder_index, chunk_slot_start_diff
),
builders AS (
  SELECT
      epoch,
      builder_index,
      toString(any(pubkey)) AS builder_pubkey
  FROM {{ index .dep "{{external}}" "canonical_beacon_state_builder" "helpers" "from" }} FINAL
  -- The epoch holding the first slot starts up to one epoch before it.
  WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 384 SECOND
      AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
  GROUP BY epoch, builder_index
),
gossip_max_bid_details AS (
  SELECT
      g.slot_start_date_time AS slot_start_date_time,
      g.slot AS slot,
      g.epoch AS epoch,
      g.epoch_start_date_time AS epoch_start_date_time,
      g.builder_index AS builder_index,
      g.chunk_slot_start_diff AS chunk_slot_start_diff,
      g.max_value AS max_value,
      g.max_block_hash AS max_block_hash,
      g.max_earliest_bid_date_time AS max_earliest_bid_date_time,
      bu.builder_pubkey AS builder_pubkey
  FROM gossip_chunk_max g
  GLOBAL INNER JOIN builders bu ON g.epoch = bu.epoch AND g.builder_index = bu.builder_index
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    chunk_slot_start_diff,
    earliest_bid_date_time,
    relay_names,
    block_hash,
    builder_pubkey,
    transaction_value AS value
FROM max_bid_details

UNION ALL

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    chunk_slot_start_diff,
    max_earliest_bid_date_time AS earliest_bid_date_time,
    CAST([], 'Array(String)') AS relay_names,
    max_block_hash AS block_hash,
    builder_pubkey,
    max_value AS value
FROM gossip_max_bid_details
