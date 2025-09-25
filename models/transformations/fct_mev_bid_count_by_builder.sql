---
table: fct_mev_bid_count_by_builder
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
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    builder_pubkey,
    count(*) AS bid_total
FROM `{{ index .dep "{{external}}" "mev_relay_bid_trace" "database" }}`.`mev_relay_bid_trace` FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, builder_pubkey
