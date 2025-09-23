---
table: int_attestation_attested_head
interval:
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - attestation
  - head
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_attestation"
  - "{{external}}.libp2p_gossipsub_beacon_attestation"
  - "{{transformation}}.int_beacon_committee_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Get the validator indices for the validators that were in the beacon committee for the slot
WITH validator_indices AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        arrayJoin(validators) AS validator_index
    FROM `{{ index .dep "{{transformation}}" "int_beacon_committee_head" "database" }}`.`int_beacon_committee_head` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

-- Get the events for the attestations that were captured
combined_events AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root,
        source_epoch,
        source_epoch_start_date_time,
        source_root,
        target_epoch,
        target_epoch_start_date_time,
        target_root,
        attesting_validator_index,
        floor(min(propagation_slot_start_diff) / 12000) AS propagation_distance
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_events_attestation" "database" }}`.`beacon_api_eth_v1_events_attestation`
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND aggregation_bits = ''
        AND attesting_validator_index IS NOT NULL
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root, source_epoch, source_epoch_start_date_time, source_root, target_epoch, target_epoch_start_date_time, target_root, attesting_validator_index

    UNION ALL

    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root,
        source_epoch,
        source_epoch_start_date_time,
        source_root,
        target_epoch,
        target_epoch_start_date_time,
        target_root,
        attesting_validator_index,
        floor(min(propagation_slot_start_diff) / 12000) AS propagation_distance
    FROM `{{ index .dep "{{external}}" "libp2p_gossipsub_beacon_attestation" "database" }}`.`libp2p_gossipsub_beacon_attestation` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND aggregation_bits = ''
        AND attesting_validator_index IS NOT NULL
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root, source_epoch, source_epoch_start_date_time, source_root, target_epoch, target_epoch_start_date_time, target_root, attesting_validator_index
),

-- Make sure we only have events for validators in the validator_indices table
filtered_events AS (
    SELECT
        ce.slot,
        ce.slot_start_date_time,
        ce.epoch,
        ce.epoch_start_date_time,
        ce.beacon_block_root,
        ce.source_epoch,
        ce.source_epoch_start_date_time,
        ce.source_root,
        ce.target_epoch,
        ce.target_epoch_start_date_time,
        ce.target_root,
        ce.attesting_validator_index,
        ce.propagation_distance
    FROM combined_events ce
    INNER JOIN validator_indices vi ON ce.slot = vi.slot AND ce.attesting_validator_index = vi.validator_index
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    source_epoch,
    source_epoch_start_date_time,
    source_root,
    target_epoch,
    target_epoch_start_date_time,
    target_root,
    beacon_block_root AS block_root,
    attesting_validator_index,
    min(propagation_distance) AS propagation_distance
FROM filtered_events
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root, source_epoch, source_epoch_start_date_time, source_root, target_epoch, target_epoch_start_date_time, target_root, attesting_validator_index
