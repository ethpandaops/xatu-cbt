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
  # fct_block_proposer_head needs live block gossip, so it starts when a network's sentries do (mainnet 2025-05-14),
  # while int_block_proposer_canonical reaches genesis. The head is in a group with the canonical model, so the range
  # follows the canonical model back to genesis. The guard below keeps the head gating the range where it exists.
  - - "{{transformation}}.int_block_proposer_canonical"
    - "{{transformation}}.fct_block_proposer_head"
---
-- Only the head knows a block was orphaned. Before the head's first slot every row comes from the canonical model
-- alone, so a slot is canonical or missed and an orphaned proposal shows as missed.
--
-- The group above no longer makes CBT wait for the head, and CBT never goes back to a range it has processed, so a
-- range read before the head reaches it would lose its orphaned blocks for good. This first statement fails the task,
-- and CBT retries it, until the head has a slot at or past the end of the range. Ranges before the head's first slot
-- pass, as the head has later slots.
SELECT throwIf(
    (
        SELECT count() = 0
        FROM (
            SELECT 1
            FROM {{ index .dep "{{transformation}}" "fct_block_proposer_head" "helpers" "from" }}
            WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.end }})
            LIMIT 1
        )
    ),
    'fct_block_proposer_head has not reached the end of this range yet, waiting for it'
);

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
    FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

head_blocks AS (
    SELECT DISTINCT
        slot,
        block_root
    FROM {{ index .dep "{{transformation}}" "fct_block_proposer_head" "helpers" "from" }} FINAL
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
GLOBAL LEFT JOIN head_blocks h ON c.slot = h.slot
