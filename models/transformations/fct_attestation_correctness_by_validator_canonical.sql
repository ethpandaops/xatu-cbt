---
table: fct_attestation_correctness_by_validator_canonical
interval:
  max: 384
schedules:
  forwardfill: "@every 30s"
tags:
  - slot
  - attestation
  - canonical
dependencies:
  - "{{transformation}}.int_attestation_attested_canonical"
  - "{{transformation}}.fct_block_proposer"
  - "{{external}}.canonical_beacon_committee"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        `status`
    FROM `{{ index .dep "{{transformation}}" "fct_block_proposer" "database" }}`.`fct_block_proposer` FINAL
    WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL '768 SECOND' -- look back 64 slots to get what block was attested to
        AND slot_start_date_time <= fromUnixTimestamp({{ .bounds.end }})
),

attestations AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        attesting_validator_index,
        inclusion_distance
    FROM `{{ index .dep "{{transformation}}" "int_attestation_attested_canonical" "database" }}`.`int_attestation_attested_canonical` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

duties AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        arrayJoin(validators) AS attesting_validator_index
    FROM `{{ index .dep "{{external}}" "canonical_beacon_committee" "database" }}`.`canonical_beacon_committee` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)

-- We need a row for every `duties` row
-- block_root and inclusion_distance is from `attestations`, if its not available then null
-- slot distance is above attestations.block_root then try to find that in `blocks`, the slot - block.slot is the slot distance
SELECT
  fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
  duties.slot AS slot,
  duties.slot_start_date_time AS slot_start_date_time,
  duties.epoch AS epoch,
  duties.epoch_start_date_time AS epoch_start_date_time,
  duties.attesting_validator_index AS attesting_validator_index,
  attestations.block_root AS block_root,
  if(attestations.block_root IS NOT NULL AND blocks.slot IS NOT NULL, 
     duties.slot - blocks.slot, 
     NULL) AS slot_distance,
  attestations.inclusion_distance,
  if (blocks.status IS NOT NULL, blocks.status, 'missed') AS status
FROM duties
LEFT JOIN attestations ON 
    duties.slot = attestations.slot 
    AND duties.attesting_validator_index = attestations.attesting_validator_index
LEFT JOIN blocks ON 
    attestations.block_root = blocks.block_root
    AND attestations.block_root IS NOT NULL
SETTINGS join_use_nulls = 1
