---
table: int_storage_slot_expiry_by_6m
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
  - "{{transformation}}.int_storage_slot_diff_by_address_slot"
  - "{{transformation}}.int_storage_slot_read"
  - "{{transformation}}.int_storage_slot_next_touch"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- Tracks storage slots eligible for expiry: slots touched 6 months ago with no subsequent access.
-- A slot expires at block N if it was last touched at timestamp(N) - 6 months and not accessed since.
-- "Touch" includes both writes (storage diffs) AND reads - any interaction resets the expiry clock.
--
-- OPTIMIZATIONS:
-- 1. Use canonical_execution_block for block→timestamp lookups (indexed by block_number)
-- 2. Use int_execution_block_by_date for timestamp→block lookups (indexed by block_date_time)
-- 3. Materialize block metadata for current bounds to avoid multiple FINAL passes
-- 4. Replace CROSS JOIN with INNER JOIN + filter for first_expiry_block_map
-- 5. Start from diff/read tables (efficient block_number index) and JOIN to get next_touch_block
-- 6. Two-phase diff lookup: use int_storage_slot_diff_latest_state for O(1) lookups,
--    fall back to historical scan only for slots modified after the 6-month window
-- 7. Filter next_touch JOIN by block_number range to use primary key index (ORDER BY block_number, ...)
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- STEP 1: Get timestamps for current bounds using canonical_execution_block (indexed by block_number)
current_bounds AS (
    SELECT
        min(block_date_time) as min_time,
        max(block_date_time) as max_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- STEP 2: Get 6-month-ago block range using int_execution_block_by_date (indexed by block_date_time)
old_block_range AS (
    SELECT
        min(block_number) as min_old_block,
        max(block_number) as max_old_block
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time >= (SELECT min_time - INTERVAL 6 MONTH - INTERVAL 1 DAY FROM current_bounds)
        AND block_date_time <= (SELECT max_time - INTERVAL 6 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- OPTIMIZATION: Materialize block metadata for current bounds (used multiple times below)
current_bounds_blocks AS (
    SELECT block_number, block_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Get all touches from 6-month-ago window (EFFICIENT - both tables ordered by block_number)
old_touches AS (
    SELECT block_number, address, slot_key
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }}
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
    UNION ALL
    SELECT block_number, address, slot_key
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }}
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
),
-- Deduplicate: a slot touched by both read and write in same block counts as one
unique_old_touches AS (
    SELECT DISTINCT block_number, address, slot_key
    FROM old_touches
),
-- Materialize old block metadata for touch_time lookups (indexed by block_number)
old_block_metadata AS (
    SELECT block_number, block_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Join touches with next_touch and block metadata
-- OPTIMIZATION: Filter next_touch by block_number range to use primary key index
-- (next_touch is ORDER BY block_number, address, slot_key)
touches_with_metadata AS (
    SELECT
        t.block_number as touch_block,
        t.address as address,
        t.slot_key as slot_key,
        nt.next_touch_block as next_touch_block,
        bm.block_date_time as touch_time
    FROM unique_old_touches t
    INNER JOIN {{ index .dep "{{transformation}}" "int_storage_slot_next_touch" "helpers" "from" }} nt FINAL
        ON t.address = nt.address
        AND t.slot_key = nt.slot_key
        AND t.block_number = nt.block_number
    INNER JOIN old_block_metadata bm
        ON t.block_number = bm.block_number
    WHERE nt.block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
),
-- Get effective_bytes using two-phase lookup to avoid scanning all historical diffs:
-- Phase 1: Use int_storage_slot_diff_latest_state (105M rows) for O(1) lookups
-- Phase 2: Fall back to historical scan ONLY for slots modified after max_old_block
--
-- This avoids the unbounded historical scan that caused memory issues.

-- Step 1: Get latest diff info for each touched slot from the helper table
slot_latest_info AS (
    SELECT
        t.address,
        t.slot_key,
        ls.block_number as latest_diff_block,
        ls.effective_bytes_to
    FROM (SELECT DISTINCT address, slot_key FROM unique_old_touches) t
    LEFT JOIN `{{ .self.database }}`.int_storage_slot_diff_latest_state ls FINAL
        ON ls.address = t.address AND ls.slot_key = t.slot_key
),
-- Step 2a: Slots where latest diff <= max_old_block → use latest_state directly (FAST PATH)
-- These slots haven't been modified since the old window, so latest_state has the right answer
slots_use_latest AS (
    SELECT
        address,
        slot_key,
        latest_diff_block as diff_block,
        effective_bytes_to as effective_bytes
    FROM slot_latest_info
    WHERE latest_diff_block IS NOT NULL
        AND latest_diff_block <= (SELECT max_old_block FROM old_block_range)
),
-- Step 2b: Slots where latest diff > max_old_block → need historical lookup (SLOW PATH)
-- These slots were modified AFTER the old window, so we need to find the diff before that
slots_need_historical AS (
    SELECT address, slot_key
    FROM slot_latest_info
    WHERE latest_diff_block IS NOT NULL
        AND latest_diff_block > (SELECT max_old_block FROM old_block_range)
),
-- Step 3: Historical lookup ONLY for the small subset that needs it
slots_from_history AS (
    SELECT
        address,
        slot_key,
        argMax(block_number, block_number) as diff_block,
        argMax(effective_bytes_to, block_number) as effective_bytes
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff_by_address_slot" "helpers" "from" }}
    WHERE block_number <= (SELECT max_old_block FROM old_block_range)
        AND (address, slot_key) IN (SELECT address, slot_key FROM slots_need_historical)
    GROUP BY address, slot_key
),
-- Step 4: Combine both paths
latest_diffs_for_candidates AS (
    SELECT address, slot_key, diff_block, effective_bytes FROM slots_use_latest
    UNION ALL
    SELECT address, slot_key, diff_block, effective_bytes FROM slots_from_history
),
candidate_expiries_raw AS (
    SELECT
        t.touch_block,
        t.touch_time,
        t.address,
        t.slot_key,
        t.next_touch_block,
        d.effective_bytes as effective_bytes
    FROM touches_with_metadata t
    LEFT JOIN latest_diffs_for_candidates d
        ON t.address = d.address
        AND t.slot_key = d.slot_key
    -- Diff must be at or before this touch (argMax gives latest overall, filter per-touch here)
    WHERE d.diff_block <= t.touch_block
    -- Slot must be active: most recent diff has bytes > 0 (not a CLEAR)
        AND d.effective_bytes > 0
),
-- Add join_key for ASOF JOIN (requires equality condition)
candidate_expiries AS (
    SELECT 1 as join_key, * FROM candidate_expiries_raw
),
-- Pre-filter blocks in bounds for expiry calculations (from materialized current bounds)
expiry_blocks_in_bounds AS (
    SELECT block_number, block_date_time
    FROM current_bounds_blocks
),
-- Build expiry threshold mapping: for each block in bounds, calculate the 6-month-ago threshold
expiry_thresholds AS (
    SELECT
        1 as join_key,
        block_number as expiry_block,
        block_date_time - INTERVAL 6 MONTH as threshold_time
    FROM expiry_blocks_in_bounds
),
-- Map candidates to their expiry block using ASOF JOIN
-- Finds the first block where threshold_time >= touch_time (slot becomes eligible for expiry)
candidates_with_expiry AS (
    SELECT
        c.touch_block,
        c.touch_time,
        c.address,
        c.slot_key,
        c.effective_bytes,
        c.next_touch_block,
        e.expiry_block
    FROM candidate_expiries c
    ASOF LEFT JOIN expiry_thresholds e
        ON c.join_key = e.join_key AND c.touch_time <= e.threshold_time
),
-- OPTIMIZATION 3: Use INNER JOIN with filter instead of CROSS JOIN
-- This is more selective and avoids cartesian product explosion
-- IMPORTANT: Use subtraction-based logic (touch_time <= block_time - 6 MONTH) to match
-- the ASOF JOIN in candidates_with_expiry. Using addition (block_time >= touch_time + 6 MONTH)
-- gives different results due to month length asymmetry (e.g., May 31 + 6 months = Nov 30,
-- but Nov 30 - 6 months = May 30, not May 31).
first_expiry_block_map AS (
    SELECT
        c.touch_time,
        min(e.block_number) as first_expiry_block
    FROM (SELECT DISTINCT touch_time FROM candidate_expiries) c
    INNER JOIN expiry_blocks_in_bounds e
        ON c.touch_time <= e.block_date_time - INTERVAL 6 MONTH
        AND c.touch_time > e.block_date_time - INTERVAL 6 MONTH - INTERVAL 1 DAY
    GROUP BY c.touch_time
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    c.expiry_block as block_number,
    c.address,
    c.slot_key,
    c.effective_bytes
FROM candidates_with_expiry c
INNER JOIN first_expiry_block_map f ON c.touch_time = f.touch_time
WHERE
    -- Guard: only process if current bounds are 6+ months after Ethereum genesis
    (SELECT min_time FROM current_bounds) >= toDateTime('2016-01-30 00:00:00')
    -- Expiry block must fall within current bounds
    AND c.expiry_block BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    -- Only expire if no touch occurred between touch and expiry:
    -- next_touch_block IS NULL means never touched again, OR
    -- next_touch_block > expiry_block means next touch is after expiry window
    AND (c.next_touch_block IS NULL OR c.next_touch_block > c.expiry_block)
    -- CRITICAL: Ensure this is the globally first valid expiry block for this candidate.
    -- The ASOF JOIN finds the first match WITHIN bounds, but due to ±1 day buffer overlap,
    -- the same candidate can be selected in multiple runs. This check ensures we only
    -- emit the expiry at the TRUE first block where touch_time + 6 months <= block_time.
    AND c.expiry_block = f.first_expiry_block
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    max_bytes_before_external_sort = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }})
AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }};
