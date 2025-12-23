---
table: int_contract_storage_expiry_1m
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
  - contract
  - expiry
  - 1m
dependencies:
  - "{{transformation}}.int_contract_storage_next_touch"
  - "{{transformation}}.int_storage_slot_state_by_address"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- Base tier (1-month expiry): Scans contract touches from 1 month ago.
-- Outputs touch_block to enable waterfall cascading to longer policies (6m, 12m, etc.).
-- A contract "expires" when NO slot in that contract has been touched for 1 month.
-- This is the contract-level equivalent of int_storage_slot_expiry_1m.
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
-- Get all contract touches from 1-month-ago window from int_contract_storage_next_touch
old_touches_with_next AS (
    SELECT
        block_number,
        address,
        argMax(next_touch_block, updated_date_time) as next_touch_block
    FROM {{ index .dep "{{transformation}}" "int_contract_storage_next_touch" "helpers" "from" }}
    WHERE block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
    GROUP BY block_number, address
),
-- Unique addresses for efficient lookups
unique_addresses AS (
    SELECT DISTINCT address
    FROM old_touches_with_next
),
-- Block metadata for old window
-- Use GROUP BY with argMax to properly deduplicate across shards when using cluster()
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
        t.next_touch_block as next_touch_block,
        bm.block_date_time as touch_time
    FROM old_touches_with_next t
    INNER JOIN old_block_metadata bm
        ON t.block_number = bm.block_number
),
-- Lookup active_slots and effective_bytes at the time of each touch from int_storage_slot_state_by_address
-- This gives cumulative state per address at the latest block <= touch_block
latest_state_for_addresses AS (
    SELECT
        address,
        argMax(active_slots, block_number) as active_slots,
        argMax(effective_bytes, block_number) as effective_bytes
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_state_by_address" "helpers" "from" }}
    WHERE block_number <= (SELECT max_old_block FROM old_block_range)
        AND address IN (SELECT address FROM unique_addresses)
    GROUP BY address
),
-- Candidate expiries: touches with valid slots/bytes at touch time
candidate_expiries AS (
    SELECT
        t.touch_block,
        t.touch_time,
        t.address,
        t.next_touch_block,
        COALESCE(s.active_slots, 0) as active_slots,
        COALESCE(s.effective_bytes, 0) as effective_bytes
    FROM touches_with_metadata t
    LEFT JOIN latest_state_for_addresses s
        ON t.address = s.address
    WHERE COALESCE(s.active_slots, 0) > 0  -- Contract has storage
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
    c.touch_block,  -- CRITICAL: propagates through waterfall chain for matching reactivations
    c.active_slots,
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
