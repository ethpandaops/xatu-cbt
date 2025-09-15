---
table: fct_attestation_correctness_canonical
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
  - "{{transformation}}.int_block_proposer_canonical"
  - "{{external}}.canonical_beacon_committee"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH slots AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root
    FROM `{{ index .dep "{{transformation}}" "int_block_proposer_canonical" "database" }}`.`int_block_proposer_canonical` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

votes_per_block_root AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        COUNT(*) as votes_actual
    FROM `{{ index .dep "{{transformation}}" "int_attestation_attested_canonical" "database" }}`.`int_attestation_attested_canonical` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
),

votes_max AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        COUNT(DISTINCT arrayJoin(validators)) as votes_max
    FROM `{{ index .dep "{{external}}" "canonical_beacon_committee" "database" }}`.`canonical_beacon_committee` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time
),

votes_per_slot AS (
    SELECT
        s.slot as slot,
        s.slot_start_date_time as slot_start_date_time,
        s.epoch as epoch,
        s.epoch_start_date_time as epoch_start_date_time,
        s.block_root as block_root,
        COALESCE(vm.votes_max, 0) as votes_max,
        COALESCE(v.votes_actual, 0) as votes_actual
    FROM slots s
    LEFT JOIN votes_per_block_root v 
        ON s.slot = v.slot 
        AND s.slot_start_date_time = v.slot_start_date_time
        AND s.epoch = v.epoch
        AND s.epoch_start_date_time = v.epoch_start_date_time
        AND s.block_root = v.block_root
    LEFT JOIN votes_max vm 
        ON s.slot = vm.slot 
        AND s.slot_start_date_time = vm.slot_start_date_time
        AND s.epoch = vm.epoch
        AND s.epoch_start_date_time = vm.epoch_start_date_time
)

SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  slot,
  slot_start_date_time,
  epoch,
  epoch_start_date_time,
  block_root,
  votes_max,
  votes_actual
FROM votes_per_slot
