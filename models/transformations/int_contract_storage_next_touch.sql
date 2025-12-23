---
table: int_contract_storage_next_touch
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
dependencies:
  - "{{transformation}}.int_storage_slot_diff"
  - "{{transformation}}.int_storage_slot_read"
---
-- Precomputes next_touch_block for each CONTRACT to enable efficient contract-level expiry range checks.
-- A "touch" includes any slot write (diff) or read on the contract.
-- This is the contract-level equivalent of int_storage_slot_next_touch.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Collect unique blocks per CONTRACT (not per slot)
touches_aggregated AS (
    SELECT
        address,
        arraySort(groupUniqArray(block_number)) as blocks,
        min(block_number) as first_block
    FROM (
        SELECT block_number, address
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number, address
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    )
    GROUP BY address
),
-- Get latest state for contracts touched in this batch using IN + argMax (faster than FINAL)
latest_state AS (
    SELECT
        address,
        argMax(block_number, updated_date_time) as block_number,
        argMax(next_touch_block, updated_date_time) as next_touch_block
    FROM `{{ .self.database }}`.helper_contract_storage_next_touch_latest_state
    WHERE address IN (SELECT address FROM touches_aggregated)
    GROUP BY address
),
-- Previous tail rows that need next_touch_block updated
-- Only match tails that are BEFORE the first block in current batch to avoid self-reference
-- when a batch is re-processed (the current tail would point to itself otherwise)
prev_tail_rows AS (
    SELECT
        ls.block_number,
        ls.address,
        a.first_block
    FROM latest_state ls
    INNER JOIN touches_aggregated a ON ls.address = a.address
    WHERE ls.next_touch_block IS NULL
        AND ls.block_number < a.first_block
),
-- Update rows for previous tail
update_rows AS (
    SELECT
        now() as updated_date_time,
        block_number,
        address,
        first_block as next_touch_block
    FROM prev_tail_rows
),
-- New rows with next_touch_block computed via window function
new_rows AS (
    SELECT
        fromUnixTimestamp({{ .task.start }}) as updated_date_time,
        block_number,
        address,
        nullIf(leadInFrame(block_number, 1, 0) OVER (
            PARTITION BY address
            ORDER BY block_number
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ), 0) as next_touch_block
    FROM (
        SELECT
            address,
            arrayJoin(blocks) as block_number
        FROM touches_aggregated
    )
)
SELECT * FROM update_rows
UNION ALL
SELECT * FROM new_rows
SETTINGS
    max_bytes_before_external_sort = 10000000000,
    max_bytes_before_external_group_by = 10000000000,
    max_threads = 8,
    distributed_aggregation_memory_efficient = 1,
    join_algorithm = 'parallel_hash';

-- Update latest_state helper table with new tail rows
INSERT INTO `{{ .self.database }}`.helper_contract_storage_next_touch_latest_state
WITH
touches_aggregated AS (
    SELECT
        address,
        arraySort(groupUniqArray(block_number)) as blocks
    FROM (
        SELECT block_number, address
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number, address
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    )
    GROUP BY address
),
new_rows AS (
    SELECT
        fromUnixTimestamp({{ .task.start }}) as updated_date_time,
        block_number,
        address,
        nullIf(leadInFrame(block_number, 1, 0) OVER (
            PARTITION BY address
            ORDER BY block_number
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ), 0) as next_touch_block
    FROM (
        SELECT
            address,
            arrayJoin(blocks) as block_number
        FROM touches_aggregated
    )
)
SELECT updated_date_time, address, block_number, next_touch_block
FROM new_rows
WHERE next_touch_block IS NULL
SETTINGS
    max_bytes_before_external_sort = 10000000000,
    max_threads = 8;
