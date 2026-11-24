---
table: fct_mev_bid_count_by_relay
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
  # From Gloas (ePBS) bids travel on gossip rather than through relays, so
  # gossip bids keep the model advancing with no relay rows to count.
  - [
    "{{external}}.mev_relay_bid_trace",
    "{{external}}.beacon_api_eth_v1_events_execution_payload_bid"
  ]
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    relay_name,
    count(*) AS bid_total
FROM {{ index .dep "{{external}}" "mev_relay_bid_trace" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, relay_name
