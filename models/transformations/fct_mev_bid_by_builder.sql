---
table: fct_mev_bid_by_builder
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
WITH max_bids AS (
  -- First, get the max value and corresponding block_hash for each builder/slot
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      builder_pubkey,
      block_hash,
      max(value) AS max_value
  FROM `{{ index .dep "{{external}}" "mev_relay_bid_trace" "database" }}`.`mev_relay_bid_trace` FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, builder_pubkey, block_hash
),
max_value AS (
  -- Then get the earliest timestamp for that specific max value and block_hash, plus relay names
  SELECT
      mb.slot_start_date_time,
      mb.slot,
      mb.epoch,
      mb.epoch_start_date_time,
      mb.builder_pubkey,
      mb.max_value AS transaction_value,
      mb.block_hash,
      min(toDateTime64(t.timestamp_ms / 1000, 3)) AS earliest_bid_date_time,
      groupArray(DISTINCT t.relay_name) AS relay_names
  FROM max_bids AS mb
  INNER JOIN `{{ index .dep "{{external}}" "mev_relay_bid_trace" "database" }}`.`mev_relay_bid_trace` AS t FINAL
    ON mb.slot = t.slot
    AND mb.builder_pubkey = t.builder_pubkey
    AND mb.max_value = t.value
    AND mb.block_hash = t.block_hash
  WHERE t.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  GROUP BY 
      mb.slot_start_date_time,
      mb.slot,
      mb.epoch,
      mb.epoch_start_date_time,
      mb.builder_pubkey,
      mb.max_value,
      mb.block_hash
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    earliest_bid_date_time,
    relay_names,
    block_hash,
    builder_pubkey,
    transaction_value AS value
FROM max_value
