---
table: int_storage_slot_reactivation_by_6m
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
  - "{{transformation}}.int_storage_slot_next_touch"
  - "{{transformation}}.int_storage_slot_expiry_by_6m"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- Tracks storage slot reactivations/cancellations: slots touched after being expired for 6+ months.
-- A slot is "un-expired" at block N if:
--   1. It's touched at block N (read OR write, including clears)
--   2. Its previous touch was > 6 months before block N
--
-- This handles TWO cases (Issues 2, 4 & 5):
--   1. Reactivation: Slot touched with value > 0 after expiry → undoes the expiry (+1 slot)
--   2. Cancellation: Slot CLEARED (value = 0) after expiry → also undoes expiry to prevent double-counting
--
-- CRITICAL (Issue 4): effective_bytes MUST come from the expiry record, NOT current on-chain value.
-- This prevents double-counting when reactivation occurs via a WRITE that changes the value.
--
-- APPROACH: "Flipped" query using next_touch_block as a precomputed pointer.
-- Instead of: "find touches in bounds, look backwards for previous touch"
-- We do: "find next_touch rows where next_touch_block points INTO current bounds"
--
-- The next_touch table has:
--   block_number (previous touch) → next_touch_block (reactivation touch)
-- So filtering WHERE next_touch_block BETWEEN bounds directly gives us reactivation candidates.
--
-- OPTIMIZATIONS:
-- 1. Use canonical_execution_block for block→timestamp lookups (indexed by block_number)
-- 2. Use int_execution_block_by_date for timestamp→block lookups (indexed by block_date_time)
-- 3. Pre-compute max_old_block (6 months before bounds) to filter by indexed block_number
-- 4. Filter next_touch by block_number (partition key + sort key) then next_touch_block
-- 5. Use full_sorting_merge join algorithm for expiry join
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- STEP 1: Get timestamp for end of current bounds
current_bounds AS (
    SELECT max(block_date_time) as max_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- STEP 2: Get max block that is 6+ months before current bounds
-- Any previous touch at or before this block has a gap >= 6 months
old_block_range AS (
    SELECT max(block_number) as max_old_block
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time <= (SELECT max_time - INTERVAL 6 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- Find reactivation candidates using next_touch_block as precomputed pointer
-- next_touch is partitioned by intDiv(block_number, 5000000) and sorted by (block_number, address, slot_key)
-- Filtering by block_number <= max_old_block uses the primary key index efficiently
reactivation_candidates AS (
    SELECT
        nt.next_touch_block as reactivation_block,
        nt.address,
        nt.slot_key,
        nt.block_number as previous_touch_block
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_next_touch" "helpers" "from" }} nt FINAL
    WHERE nt.block_number <= (SELECT max_old_block FROM old_block_range)
        AND nt.next_touch_block BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
)
-- Join with expiry table and use argMax to get most recent expiry (avoids ORDER BY + LIMIT 1 BY)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    r.reactivation_block as block_number,
    r.address,
    r.slot_key,
    -- CRITICAL: Use the expiry amount, NOT current on-chain value (Issue 4)
    -- argMax gets effective_bytes from the most recent expiry record
    argMax(e.effective_bytes, e.block_number) as effective_bytes
FROM reactivation_candidates r
-- INNER JOIN ensures we only emit reactivations for slots that actually expired
INNER JOIN {{ index .dep "{{transformation}}" "int_storage_slot_expiry_by_6m" "helpers" "from" }} e FINAL
    ON r.address = e.address
    AND r.slot_key = e.slot_key
    AND e.block_number < r.reactivation_block
    -- The expiry must have occurred after the previous touch (the one that started the expiry window)
    AND e.block_number > r.previous_touch_block
GROUP BY r.reactivation_block, r.address, r.slot_key
SETTINGS
    join_algorithm = 'full_sorting_merge',
    max_bytes_before_external_group_by = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }})
AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }};
