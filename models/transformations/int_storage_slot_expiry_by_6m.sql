---
table: int_storage_slot_expiry_by_6m
type: incremental
interval:
  type: block
  max: 10000
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
  - "{{transformation}}.int_storage_slot_diff_by_address_slot"
  - "{{transformation}}.int_storage_slot_read"
  - "{{transformation}}.int_storage_slot_next_touch"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- Tracks storage slots eligible for expiry: slots touched 6 months ago with no subsequent access.
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
-- Get 6-month-ago block range
old_block_range AS (
    SELECT
        min(block_number) as min_old_block,
        max(block_number) as max_old_block
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time >= (SELECT min_time - INTERVAL 6 MONTH - INTERVAL 1 DAY FROM current_bounds)
        AND block_date_time <= (SELECT max_time - INTERVAL 6 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- Block metadata for current bounds
current_bounds_blocks AS (
    SELECT block_number, block_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Get all touches from 6-month-ago window
old_touches AS (
    SELECT block_number, address, slot_key
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }}
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
    UNION ALL
    SELECT block_number, address, slot_key
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }}
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
),
-- Deduplicate touches per block
unique_old_touches AS (
    SELECT DISTINCT block_number, address, slot_key
    FROM old_touches
),
-- Block metadata for old window
old_block_metadata AS (
    SELECT block_number, block_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Join touches with next_touch and block metadata
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
-- Two-phase effective_bytes lookup: use latest_state helper, fall back to historical for recently modified slots
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
-- Slots where latest diff is in or before old window - use latest_state directly
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
-- Slots modified after old window - need historical lookup
slots_need_historical AS (
    SELECT address, slot_key
    FROM slot_latest_info
    WHERE latest_diff_block IS NOT NULL
        AND latest_diff_block > (SELECT max_old_block FROM old_block_range)
),
-- Historical lookup for slots that need it
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
-- Combine both lookup paths
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
    WHERE d.diff_block <= t.touch_block
        AND d.effective_bytes > 0
),
candidate_expiries AS (
    SELECT 1 as join_key, * FROM candidate_expiries_raw
),
-- Blocks in bounds for expiry calculations
expiry_blocks_in_bounds AS (
    SELECT block_number, block_date_time
    FROM current_bounds_blocks
),
-- Expiry threshold per block (6 months ago)
expiry_thresholds AS (
    SELECT
        1 as join_key,
        block_number as expiry_block,
        block_date_time - INTERVAL 6 MONTH as threshold_time
    FROM expiry_blocks_in_bounds
),
-- Map candidates to their expiry block via ASOF JOIN
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
-- Map touch_time to first valid expiry block (uses subtraction for month length consistency)
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
    -- Only process if bounds are 6+ months after Ethereum genesis
    (SELECT min_time FROM current_bounds) >= toDateTime('2016-01-30 00:00:00')
    AND c.expiry_block BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    -- No touch between original touch and expiry
    AND (c.next_touch_block IS NULL OR c.next_touch_block > c.expiry_block)
    -- Must be the first valid expiry block for this candidate
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
