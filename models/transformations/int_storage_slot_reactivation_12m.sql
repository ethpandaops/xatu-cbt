---
table: int_storage_slot_reactivation_12m
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
  - reactivation
  - 12m
dependencies:
  - "{{transformation}}.int_storage_slot_next_touch"
  - "{{transformation}}.int_storage_slot_expiry_12m"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- 12-month reactivation: Detects when a slot is touched after its 12m expiry event.
-- A reactivation occurs when next_touch_block falls within current bounds
-- and there was a 12m expiry between the previous touch and the reactivation.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
current_bounds AS (
    SELECT max(block_date_time) as max_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Max block that is 12+ months before current bounds
old_block_range AS (
    SELECT max(block_number) as max_old_block
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time <= (SELECT max_time - INTERVAL 12 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- Reactivation candidates: previous touch was > 12 months ago
-- NO FINAL: uses projection on next_touch_block, deduplicates via argMax
reactivation_candidates AS (
    SELECT
        argMax(next_touch_block, updated_date_time) as reactivation_block,
        address,
        slot_key,
        block_number as previous_touch_block
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_next_touch" "helpers" "from" }}
    WHERE next_touch_block BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND block_number <= (SELECT max_old_block FROM old_block_range)
    GROUP BY block_number, address, slot_key
),
-- Extract unique address/slot pairs for semi-join optimization
unique_pairs AS (
    SELECT DISTINCT address, slot_key
    FROM reactivation_candidates
),
-- Calculate global bounds for expiry block range filter
expiry_bounds AS (
    SELECT
        min(previous_touch_block) as min_prev,
        max(reactivation_block) as max_react
    FROM reactivation_candidates
),
-- Pre-filter 12m expiry table using IN clause, dedupe via argMax (avoids FINAL overhead)
filtered_expiry AS (
    SELECT
        address,
        slot_key,
        block_number,
        touch_block,
        argMax(effective_bytes, updated_date_time) as effective_bytes
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_12m" "helpers" "from" }}
    WHERE block_number > (SELECT min_prev FROM expiry_bounds)
        AND block_number < (SELECT max_react FROM expiry_bounds)
        AND (address, slot_key) IN (SELECT address, slot_key FROM unique_pairs)
    GROUP BY address, slot_key, block_number, touch_block
)
-- Join with filtered expiry table to get effective_bytes from the expiry record
-- Uses argMax to get the most recent expiry's effective_bytes if multiple exist
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    r.reactivation_block as block_number,
    r.address,
    r.slot_key,
    argMax(e.touch_block, e.block_number) as touch_block,
    argMax(e.effective_bytes, e.block_number) as effective_bytes
FROM reactivation_candidates r
INNER JOIN filtered_expiry e
    ON r.address = e.address
    AND r.slot_key = e.slot_key
    AND e.block_number < r.reactivation_block
    AND e.block_number > r.previous_touch_block
GROUP BY r.reactivation_block, r.address, r.slot_key
SETTINGS
    join_algorithm = 'parallel_hash',
    max_bytes_before_external_group_by = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1;
