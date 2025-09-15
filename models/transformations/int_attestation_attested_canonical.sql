---
table: int_attestation_attested_canonical
interval:
  max: 384
schedules:
  forwardfill: "@every 5s"
tags:
  - slot
  - attestation
  - head
dependencies:
  - "{{external}}.canonical_beacon_elaborated_attestation"
  - "{{external}}.canonical_beacon_committee"
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
    FROM `{{ index .dep "{{external}}" "canonical_beacon_committee" "database" }}`.`canonical_beacon_committee` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

-- Get the events for the attestations that were captured
attestations AS (
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
        arrayJoin(validators) AS attesting_validator_index,
        block_slot - slot AS inclusion_distance
    FROM `{{ index .dep "{{external}}" "canonical_beacon_elaborated_attestation" "database" }}`.`canonical_beacon_elaborated_attestation` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

-- Make sure we only have events for validators in the validator_indices table
filtered_events AS (
    SELECT
        a.slot,
        a.slot_start_date_time,
        a.epoch,
        a.epoch_start_date_time,
        a.beacon_block_root,
        a.source_epoch,
        a.source_epoch_start_date_time,
        a.source_root,
        a.target_epoch,
        a.target_epoch_start_date_time,
        a.target_root,
        a.attesting_validator_index,
        a.inclusion_distance
    FROM attestations AS a
    INNER JOIN validator_indices vi ON a.slot = vi.slot AND a.attesting_validator_index = vi.validator_index
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
    inclusion_distance
FROM attestations
