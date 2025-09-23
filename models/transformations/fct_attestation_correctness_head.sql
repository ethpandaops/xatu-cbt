---
table: fct_attestation_correctness_head
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
  - "{{transformation}}.int_beacon_committee_head"
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
    FROM `{{ index .dep "{{transformation}}" "int_block_proposer_head" "database" }}`.`int_block_proposer_head` FINAL
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
    FROM `{{ index .dep "{{transformation}}" "int_attestation_attested_head" "database" }}`.`int_attestation_attested_head` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
),

total_votes_per_slot AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        COUNT(*) as total_votes
    FROM `{{ index .dep "{{transformation}}" "int_attestation_attested_head" "database" }}`.`int_attestation_attested_head` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time
),

votes_max AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        COUNT(DISTINCT arrayJoin(validators)) as votes_max
    FROM `{{ index .dep "{{transformation}}" "int_beacon_committee_head" "database" }}`.`int_beacon_committee_head` FINAL
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
        COALESCE(v.votes_actual, NULL) as votes_actual,
        CASE 
            WHEN tv.total_votes IS NULL THEN NULL
            WHEN v.votes_actual IS NULL THEN toUInt64(tv.total_votes)
            ELSE toUInt64(tv.total_votes - v.votes_actual)
        END as votes_other
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
    LEFT JOIN total_votes_per_slot tv
        ON s.slot = tv.slot 
        AND s.slot_start_date_time = tv.slot_start_date_time
        AND s.epoch = tv.epoch
        AND s.epoch_start_date_time = tv.epoch_start_date_time
)

SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  slot,
  slot_start_date_time,
  epoch,
  epoch_start_date_time,
  block_root,
  votes_max,
  votes_actual,
  votes_other
FROM votes_per_slot
SETTINGS join_use_nulls = 1
