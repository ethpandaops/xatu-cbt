---
table: fct_attestation_correctness_by_validator_head
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
  - "{{transformation}}.int_attestation_attested_head"
  - "{{transformation}}.int_block_proposer_head"
  - "{{external}}.beacon_api_eth_v1_beacon_committee"
lag: 12
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root
    FROM `{{ index .dep "{{transformation}}" "int_block_proposer_head" "database" }}`.`int_block_proposer_head` FINAL
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
        propagation_distance
    FROM `{{ index .dep "{{transformation}}" "int_attestation_attested_head" "database" }}`.`int_attestation_attested_head` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

duties AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        arrayJoin(validators) AS attesting_validator_index
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_committee" "database" }}`.`beacon_api_eth_v1_beacon_committee` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)

-- We need a row for every `duties` row
-- block_root and propagation_distance is from `attestations`, if its not available then null
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
  attestations.propagation_distance
FROM duties
LEFT JOIN attestations ON 
    duties.slot = attestations.slot 
    AND duties.attesting_validator_index = attestations.attesting_validator_index
LEFT JOIN blocks ON 
    attestations.block_root = blocks.block_root
    AND attestations.block_root IS NOT NULL
SETTINGS join_use_nulls = 1
