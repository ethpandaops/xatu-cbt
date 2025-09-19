---
table: fct_mev_bid_value_by_builder
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
WITH max_value AS (
  SELECT
      slot_start_date_time,
      slot,
      epoch,
      epoch_start_date_time,
      min(toDateTime64(timestamp_ms / 1000, 3)) AS earliest_bid_date_time,
      groupArray(DISTINCT relay_name) AS relay_names,
      block_hash,
      max(value) AS transaction_value
  FROM `{{ index .dep "{{external}}" "mev_relay_bid_trace" "database" }}`.`mev_relay_bid_trace` FINAL
  GROUP BY slot_start_date_time, slot, epoch,  epoch_start_date_time,  block_hash
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
    transaction_value AS value
FROM max_value
