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
  forwardfill: "@every 1s"
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
-- 1. Update previous "tail" rows: find rows before this batch with NULL next_touch_block
--    for slots touched in this batch, and set their next_touch_block to the first touch.
-- 2. Insert new rows: add rows for this batch with next_touch_block computed via window function.
--
-- Architecture:
-- - Main table (int_storage_slot_next_touch): Full history, ORDER BY (block_number, address, slot_key)
-- - Helper table (int_storage_slot_latest_state): Latest state per slot, ORDER BY (address, slot_key)
-- - This model writes to BOTH tables atomically for consistency
--
-- Optimization notes:
-- - Uses int_storage_slot_latest_state for O(unique_slots) prev_tail lookups instead of O(total_history)
-- - Uses groupUniqArray for single-pass aggregation (avoids separate DISTINCT + GROUP BY)
-- - Uses parallel_hash join algorithm for better parallelism
-- - References: https://clickhouse.com/docs/best-practices/minimize-optimize-joins
--              https://kb.altinity.com/altinity-kb-queries-and-syntax/distinct-vs-group-by-vs-limit-by/
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Single aggregation pass: collect unique blocks per slot and compute first_block
-- groupUniqArray is more efficient than UNION ALL -> DISTINCT -> separate GROUP BY
-- This reduces source table scans from 3x to 2x (ClickHouse inlines CTEs)
touches_aggregated AS (
    SELECT
        address,
        slot_key,
        groupUniqArray(block_number) as blocks,
        min(block_number) as first_block
    FROM (
        SELECT block_number, address, slot_key
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number, address, slot_key
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    )
    GROUP BY address, slot_key
),
-- Find previous "tail" rows: slots with NULL next_touch_block that are touched in this batch.
-- These need their next_touch_block set to first_block from current batch.
--
-- OPTIMIZATION: Queries int_storage_slot_latest_state instead of main table.
-- - latest_state has ONE row per slot (56M rows) vs main table (290M+ rows)
-- - ORDER BY (address, slot_key) enables efficient point lookups
-- - O(unique_slots_in_batch) instead of O(total_history)
prev_tail_rows AS (
    SELECT
        ls.block_number,
        ls.address,
        ls.slot_key,
        a.first_block
    FROM `{{ .self.database }}`.int_storage_slot_latest_state ls FINAL
    INNER JOIN touches_aggregated a ON ls.address = a.address AND ls.slot_key = a.slot_key
    WHERE ls.next_touch_block IS NULL
),
-- Phase 1: Update rows - set next_touch_block for previous tail rows
update_rows AS (
    SELECT
        now() as updated_date_time,
        block_number,
        address,
        slot_key,
        first_block as next_touch_block
    FROM prev_tail_rows
),
-- Phase 2: New rows for this batch with next_touch_block computed via window function
-- Explode aggregated blocks back to individual rows, then compute next touch via leadInFrame
-- leadInFrame gives the next block_number within the same (address, slot_key) partition
-- nullIf converts the default value 0 to NULL for rows at end of partition (no subsequent touch)
new_rows AS (
    SELECT
        fromUnixTimestamp({{ .task.start }}) as updated_date_time,
        block_number,
        address,
        slot_key,
        nullIf(leadInFrame(block_number, 1, 0) OVER (
            PARTITION BY address, slot_key
            ORDER BY block_number
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ), 0) as next_touch_block
    FROM (
        SELECT
            address,
            slot_key,
            arrayJoin(blocks) as block_number
        FROM touches_aggregated
    )
)
-- Combine update rows and new rows for main table
SELECT * FROM update_rows
UNION ALL
SELECT * FROM new_rows
SETTINGS
    max_bytes_before_external_sort = 10000000000,
    max_bytes_before_external_group_by = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1,
    join_algorithm = 'parallel_hash';

-- INSERT 2: Update latest_state helper table
-- Only insert rows that are the NEW latest for their slot (next_touch_block IS NULL).
-- These are the "tail" rows of the current batch that will be looked up in the next batch.
-- ReplacingMergeTree(updated_date_time) will deduplicate by (address, slot_key).
INSERT INTO `{{ .self.database }}`.int_storage_slot_latest_state
WITH
touches_aggregated AS (
    SELECT
        address,
        slot_key,
        groupUniqArray(block_number) as blocks
    FROM (
        SELECT block_number, address, slot_key
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number, address, slot_key
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    )
    GROUP BY address, slot_key
),
new_rows AS (
    SELECT
        fromUnixTimestamp({{ .task.start }}) as updated_date_time,
        block_number,
        address,
        slot_key,
        nullIf(leadInFrame(block_number, 1, 0) OVER (
            PARTITION BY address, slot_key
            ORDER BY block_number
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ), 0) as next_touch_block
    FROM (
        SELECT
            address,
            slot_key,
            arrayJoin(blocks) as block_number
        FROM touches_aggregated
    )
)
-- Only insert the "tail" rows (last touch per slot in this batch)
SELECT updated_date_time, address, slot_key, block_number, next_touch_block
FROM new_rows
WHERE next_touch_block IS NULL
SETTINGS
    max_bytes_before_external_sort = 10000000000,
    max_threads = 8;
