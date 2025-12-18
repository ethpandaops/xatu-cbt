---
table: int_storage_slot_expiry_1m
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1m"
tags:
  - execution
  - storage
  - expiry
  - 1m
dependencies:
  - "{{transformation}}.int_storage_slot_diff_by_address_slot"
  - "{{transformation}}.int_storage_slot_next_touch"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- Base tier (1-month expiry): Scans raw touches from 1 month ago.
-- Outputs touch_block to enable waterfall cascading to longer policies (6m, 12m, etc.).
-- Downstream tiers check per-tier reactivation tables instead of propagated next_touch_block.
-- A "touch" includes both writes (diffs) and reads.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get timestamps for current bounds
current_bounds AS (
    SELECT
        min(block_date_time) as min_time,
        max(block_date_time) as max_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Get 1-month-ago block range
old_block_range AS (
    SELECT
        min(block_number) as min_old_block,
        max(block_number) as max_old_block
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time >= (SELECT min_time - INTERVAL 1 MONTH - INTERVAL 1 DAY FROM current_bounds)
        AND block_date_time <= (SELECT max_time - INTERVAL 1 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- Block candidates for finding expiry blocks
-- Extended range before current bounds to find true global first expiry block
expiry_block_candidates AS (
    SELECT block_number, block_date_time
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time >= (SELECT min_time - INTERVAL 1 DAY FROM current_bounds)
        AND block_date_time <= (SELECT max_time FROM current_bounds)
),
-- Get all touches from 1-month-ago window directly from int_storage_slot_next_touch
-- This is simpler than reading from diff + read separately since next_touch already has all touches
old_touches_with_next AS (
    SELECT
        block_number,
        address,
        slot_key,
        argMax(next_touch_block, updated_date_time) as next_touch_block
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_next_touch" "helpers" "from" }}
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
    GROUP BY block_number, address, slot_key
),
-- Unique address/slot pairs for efficient lookups
unique_pairs AS (
    SELECT DISTINCT address, slot_key
    FROM old_touches_with_next
),
-- Block metadata for old window
-- Use GROUP BY with argMax to properly deduplicate across shards when using cluster()
-- FINAL only deduplicates locally within each shard, not across the distributed result
old_block_metadata AS (
    SELECT
        block_number,
        argMax(block_date_time, updated_date_time) as block_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }}
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY block_number
),
-- Join touches with block metadata
touches_with_metadata AS (
    SELECT
        t.block_number as touch_block,
        t.address as address,
        t.slot_key as slot_key,
        t.next_touch_block as next_touch_block,
        bm.block_date_time as touch_time
    FROM old_touches_with_next t
    INNER JOIN old_block_metadata bm
        ON t.block_number = bm.block_number
),
-- Lookup effective_bytes at the time of each touch from historical diffs
-- Uses int_storage_slot_diff_by_address_slot which is ordered by (address, slot_key, block_number)
-- for efficient filtering and argMax aggregation
latest_diffs_for_candidates AS (
    SELECT
        address,
        slot_key,
        argMax(block_number, block_number) as diff_block,
        argMax(effective_bytes_to, block_number) as effective_bytes
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff_by_address_slot" "helpers" "from" }}
    WHERE block_number <= (SELECT max_old_block FROM old_block_range)
        AND (address, slot_key) IN (SELECT address, slot_key FROM unique_pairs)
    GROUP BY address, slot_key
),
-- Candidate expiries: touches with valid effective_bytes at touch time
candidate_expiries AS (
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
    WHERE d.diff_block <= t.touch_block
        AND d.effective_bytes > 0
),
-- Map touch_time to the GLOBAL first block where it becomes 1 month old
-- Only include if that block falls within current bounds (deterministic per touch)
first_expiry_block_map AS (
    SELECT
        c.touch_time,
        min(b.block_number) as first_expiry_block
    FROM (SELECT DISTINCT touch_time FROM candidate_expiries) c
    INNER JOIN expiry_block_candidates b
        ON b.block_date_time >= c.touch_time + INTERVAL 1 MONTH
    GROUP BY c.touch_time
    HAVING first_expiry_block BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    f.first_expiry_block as block_number,
    c.address,
    c.slot_key,
    c.touch_block,  -- CRITICAL: propagates through waterfall chain for matching reactivations
    c.effective_bytes
FROM candidate_expiries c
INNER JOIN first_expiry_block_map f ON c.touch_time = f.touch_time
WHERE
    -- No touch between original touch and expiry
    (c.next_touch_block IS NULL OR c.next_touch_block > f.first_expiry_block)
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    max_bytes_before_external_sort = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1;
