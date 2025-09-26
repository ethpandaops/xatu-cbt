---
table: fct_mev_bid_highest_value_by_builder_chunked_50ms
interval:
  max: 384
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
  FROM `{{ index .dep "{{external}}" "mev_relay_bid_trace" "database" }}`.`mev_relay_bid_trace` FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
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
  INNER JOIN max_value_per_chunk AS mvc
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
  INNER JOIN bids AS b
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
  INNER JOIN relay_aggregation AS ra
    ON bc.slot = ra.slot
    AND bc.builder_pubkey = ra.builder_pubkey
    AND bc.block_hash = ra.block_hash
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