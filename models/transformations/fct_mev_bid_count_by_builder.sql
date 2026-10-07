---
table: fct_mev_bid_count_by_builder
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
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    builder_pubkey,
    COUNT(DISTINCT block_hash) AS bid_total
FROM {{ index .dep "{{external}}" "mev_relay_bid_trace" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, slot, epoch, epoch_start_date_time, builder_pubkey

UNION ALL

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    b.slot,
    b.slot_start_date_time,
    b.epoch,
    b.epoch_start_date_time,
    bu.builder_pubkey,
    COUNT(DISTINCT b.block_hash) AS bid_total
FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload_bid" "helpers" "from" }} AS b FINAL
GLOBAL INNER JOIN (
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
) AS bu ON b.epoch = bu.epoch AND b.builder_index = bu.builder_index
WHERE b.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND b.meta_network_name = '{{ .env.NETWORK }}'
GROUP BY b.slot_start_date_time, b.slot, b.epoch, b.epoch_start_date_time, bu.builder_pubkey
