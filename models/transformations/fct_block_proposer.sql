---
table: fct_block_proposer
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - proposer
  - canonical
dependencies:
  - "{{transformation}}.int_block_proposer_canonical"
  - "{{transformation}}.fct_block_proposer_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH canonical AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        proposer_validator_index,
        proposer_pubkey
    FROM `{{ index .dep "{{transformation}}" "int_block_proposer_canonical" "database" }}`.`int_block_proposer_canonical` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

head_blocks AS (
    SELECT DISTINCT
        slot,
        block_root
    FROM `{{ index .dep "{{transformation}}" "fct_block_proposer_head" "database" }}`.`fct_block_proposer_head` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND block_root IS NOT NULL
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    c.slot,
    c.slot_start_date_time,
    c.epoch,
    c.epoch_start_date_time,
    c.proposer_validator_index,
    c.proposer_pubkey,
    CASE
        WHEN c.block_root IS NOT NULL THEN c.block_root
        WHEN h.block_root IS NOT NULL THEN h.block_root
        ELSE NULL
    END as block_root,
    CASE
        WHEN c.block_root IS NOT NULL THEN 'canonical'
        WHEN c.block_root IS NULL AND h.block_root IS NOT NULL THEN 'orphaned'
        ELSE 'missed'
    END as status
FROM canonical c
LEFT JOIN head_blocks h ON c.slot = h.slot
