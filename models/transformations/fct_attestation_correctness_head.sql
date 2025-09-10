---
table: fct_attestation_correctness_head
interval:
  max: 384
schedules:
  forwardfill: "@every 5s"
tags:
  - slot
  - attestation
  - head
dependencies:
  - "{{transformation}}.int_attestation_attested_head"
  - "{{transformation}}.int_block_proposer_head"
  - "{{transformation}}.int_block_blob_count_head"
  - "{{external}}.beacon_api_eth_v1_beacon_committee"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH slots AS (
    SELECT
        slot,
        epoch,
        block_root
    FROM `{{ index .dep "{{transformation}}" "int_block_proposer_head" "database" }}`.`int_block_proposer_head` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

votes_per_block_root AS (
    SELECT
        slot,
        epoch,
        block_root,
        COUNT(*) as votes_actual
    FROM `{{ index .dep "{{transformation}}" "int_attestation_attested_head" "database" }}`.`int_attestation_attested_head` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, epoch, block_root
),

votes_max AS (
    SELECT
        slot,
        epoch,
        COUNT(DISTINCT arrayJoin(validators)) as votes_max
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_committee" "database" }}`.`beacon_api_eth_v1_beacon_committee` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, epoch
),

votes_per_slot AS (
    SELECT
        s.slot as slot,
        s.epoch as epoch,
        s.block_root as block_root,
        COALESCE(vm.votes_max, 0) as votes_max,
        COALESCE(v.votes_actual, 0) as votes_actual
    FROM slots s
    LEFT JOIN votes_per_block_root v 
        ON s.slot = v.slot 
        AND s.epoch = v.epoch 
        AND s.block_root = v.block_root
    LEFT JOIN votes_max vm 
        ON s.slot = vm.slot 
        AND s.epoch = vm.epoch
)

SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  slot,
  epoch,
  block_root,
  votes_max,
  votes_actual
FROM votes_per_slot
