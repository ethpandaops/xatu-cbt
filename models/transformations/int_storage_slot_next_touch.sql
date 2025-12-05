---
table: int_storage_slot_next_touch
type: incremental
interval:
  type: block
  max: 100000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5s"
tags:
  - execution
  - storage
dependencies:
  - "{{transformation}}.int_storage_slot_diff"
  - "{{transformation}}.int_storage_slot_read"
---
-- Precomputes next_touch_block for each slot touch to enable O(1) expiry range checks.
-- For each (address, slot_key, block_number), stores the next block where this slot was touched.
-- A "touch" includes both writes (diffs) AND reads - slots accessed in any way reset the expiry clock.
-- This allows expiry checks to be a simple comparison instead of array scanning.
--
-- Two-phase insert:
-- 1. Update previous "tail" rows: find the last row before this batch for each slot touched
--    in this batch, and set its next_touch_block to the first touch in this batch.
-- 2. Insert new rows: add rows for this batch with next_touch_block computed via window function.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Combine writes (diffs) and reads into a single touch stream
-- We only need block_number, address, slot_key for touch tracking
all_touches_in_batch AS (
    SELECT block_number, address, slot_key
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, slot_key
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Deduplicate: a slot touched multiple times in the same block counts as one touch
touches_in_batch AS (
    SELECT DISTINCT block_number, address, slot_key
    FROM all_touches_in_batch
),
-- Get unique slots that have touches in this batch
slots_in_batch AS (
    SELECT DISTINCT address, slot_key
    FROM touches_in_batch
),
-- For each slot touched in this batch, find the first touch block in this batch
first_touch_in_batch AS (
    SELECT
        address,
        slot_key,
        min(block_number) as first_block
    FROM touches_in_batch
    GROUP BY address, slot_key
),
-- Find the previous "tail" row for each slot (the last row before this batch with NULL next_touch_block)
-- These need to be updated to point to the first touch in this batch
prev_tail_rows AS (
    SELECT
        t.block_number,
        t.address,
        t.slot_key
    FROM `{{ .self.database }}`.`{{ .self.table }}` t FINAL
    INNER JOIN slots_in_batch s ON t.address = s.address AND t.slot_key = s.slot_key
    WHERE t.block_number < {{ .bounds.start }}
        AND t.next_touch_block IS NULL
),
-- Phase 1: Update rows - set next_touch_block for previous tail rows
update_rows AS (
    SELECT
        now() as updated_date_time,
        p.block_number,
        p.address,
        p.slot_key,
        f.first_block as next_touch_block
    FROM prev_tail_rows p
    INNER JOIN first_touch_in_batch f ON p.address = f.address AND p.slot_key = f.slot_key
),
-- Phase 2: New rows for this batch with next_touch_block computed via window function
-- leadInFrame gives the next block_number within the same (address, slot_key) partition
-- nullIf converts the default value 0 to NULL for rows at end of partition
new_rows AS (
    SELECT
        now() as updated_date_time,
        block_number,
        address,
        slot_key,
        nullIf(leadInFrame(block_number, 1, 0) OVER (
            PARTITION BY address, slot_key
            ORDER BY block_number
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ), 0) as next_touch_block
    FROM touches_in_batch
)
-- Combine update rows and new rows
SELECT * FROM update_rows
UNION ALL
SELECT * FROM new_rows
