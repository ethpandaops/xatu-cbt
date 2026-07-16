---
table: fct_block_payload_ptc_vote_head
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - payload
  - ptc
  - epbs
  - head
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_payload_attestation"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas (ePBS): Payload Timeliness Committee votes observed on the live event
-- stream, aggregated per attested block. Each PTC validator broadcasts one
-- payload attestation message per slot; multiple sentries observe the same
-- message, so counts deduplicate on validator index. slot here is the attested
-- slot (the event stream reports it directly).
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    beacon_block_root AS block_root,
    uniqExact(validator_index) AS ptc_validators_seen,
    uniqExactIf(validator_index, payload_present) AS payload_present_votes,
    uniqExactIf(validator_index, blob_data_available) AS blob_data_available_votes,
    MIN(propagation_slot_start_diff) AS first_seen_slot_start_diff,
    MAX(propagation_slot_start_diff) AS last_seen_slot_start_diff
FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_payload_attestation" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root
