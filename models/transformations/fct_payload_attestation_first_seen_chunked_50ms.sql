---
table: fct_payload_attestation_first_seen_chunked_50ms
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - payload
  - ptc
  - epbs
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_payload_attestation"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas (ePBS): PTC payload attestation arrivals in 50ms chunks, the payload
-- analogue of fct_attestation_first_seen_chunked_50ms. Each PTC validator
-- broadcasts one message per slot and multiple sentries observe it, so a
-- validator counts once at its earliest observation.
WITH first_seen AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root AS block_root,
        validator_index,
        MIN(propagation_slot_start_diff) AS seen_slot_start_diff
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_payload_attestation" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND propagation_slot_start_diff < 12000
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root, validator_index
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    toUInt32(floor(seen_slot_start_diff / 50) * 50) AS chunk_slot_start_diff,
    COUNT() AS attestation_count
FROM first_seen
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, chunk_slot_start_diff
