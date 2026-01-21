---
table: int_contract_storage_expiry_6m
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
  - 6m
dependencies:
  - "{{transformation}}.int_contract_storage_expiry_1m"
  - "{{transformation}}.int_contract_storage_reactivation_1m"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- 6-month expiry tier: Waterfalls from 1m expiries.
-- Scans 5 months of 1m expiries (not 6 months of raw touches).
-- A contract expires at 6m if it was 1m-expired 5 months ago AND not reactivated before 6m expiry.
-- This is the contract-level equivalent of int_storage_slot_expiry_6m.
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
-- Get 5-month-ago block range (for 1m expiries that are now 6m old)
source_block_range AS (
    SELECT
        min(block_number) as min_source_block,
        max(block_number) as max_source_block
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time >= (SELECT min_time - INTERVAL 5 MONTH - INTERVAL 1 DAY FROM current_bounds)
        AND block_date_time <= (SELECT max_time - INTERVAL 5 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- Block candidates for finding new expiry blocks (dummy_key enables ASOF JOIN)
expiry_block_candidates AS (
    SELECT 1 as dummy_key, block_number, block_date_time
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time >= (SELECT min_time - INTERVAL 1 DAY FROM current_bounds)
        AND block_date_time <= (SELECT max_time FROM current_bounds)
),
-- Get 1m expiries from 5 months ago (these are candidates for 6m expiry)
source_expiries AS (
    SELECT
        block_number as source_expiry_block,
        address,
        touch_block,  -- Original touch that led to 1m expiry
        active_slots,
        effective_bytes
    FROM {{ index .dep "{{transformation}}" "int_contract_storage_expiry_1m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN (SELECT min_source_block FROM source_block_range)
                           AND (SELECT max_source_block FROM source_block_range)
),
-- Get unique source blocks for efficient remote lookup
source_blocks AS (
    SELECT DISTINCT source_expiry_block as block_number
    FROM source_expiries
),
-- Get timestamps using GLOBAL IN to push filter to remote cluster (avoids full table scan)
source_block_times AS (
    SELECT
        block_number,
        argMax(block_date_time, updated_date_time) as block_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }}
    WHERE block_number GLOBAL IN (SELECT block_number FROM source_blocks)
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY block_number
),
-- Join locally with timestamps (dummy_key enables ASOF JOIN)
source_with_time AS (
    SELECT
        1 as dummy_key,
        e.source_expiry_block,
        e.address,
        e.touch_block,
        e.active_slots,
        e.effective_bytes,
        b.block_date_time as source_expiry_time
    FROM source_expiries e
    INNER JOIN source_block_times b
        ON e.source_expiry_block = b.block_number
),
-- Map source expiry time to new expiry block using ASOF JOIN
new_expiry_block_map AS (
    SELECT
        s.source_expiry_block,
        s.address,
        s.touch_block,
        s.active_slots,
        s.effective_bytes,
        s.source_expiry_time,
        c.block_number as new_expiry_block
    FROM source_with_time s
    ASOF LEFT JOIN expiry_block_candidates c
        ON s.dummy_key = c.dummy_key
        AND c.block_date_time >= s.source_expiry_time + INTERVAL 5 MONTH
    WHERE c.block_number IS NOT NULL
        AND c.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Get 1m reactivations that could invalidate these 6m expiries
relevant_reactivations AS (
    SELECT
        address,
        touch_block,
        block_number as reactivation_block
    FROM {{ index .dep "{{transformation}}" "int_contract_storage_reactivation_1m" "helpers" "from" }} FINAL
    WHERE (address, touch_block) IN (
        SELECT address, touch_block FROM new_expiry_block_map
    )
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    m.new_expiry_block as block_number,
    m.address,
    m.touch_block,  -- Propagate original touch_block for next waterfall tier
    m.active_slots,
    m.effective_bytes
FROM new_expiry_block_map m
LEFT JOIN relevant_reactivations r
    ON m.address = r.address
    AND m.touch_block = r.touch_block
    AND r.reactivation_block < m.new_expiry_block
WHERE
    -- No reactivation before 6m expiry block
    r.address = ''
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1,
    do_not_merge_across_partitions_select_final = 1;
