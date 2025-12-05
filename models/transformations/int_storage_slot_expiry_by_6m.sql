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
  - "{{transformation}}.int_storage_slot_next_touch"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- Tracks storage slots eligible for expiry: slots written 6 months ago with no subsequent access.
-- A slot expires at block N if it was last written at timestamp(N) - 6 months and not accessed since.
-- "Access" includes both writes (storage diffs) AND reads - any interaction resets the expiry clock.
--
-- Optimization: Uses int_storage_slot_next_touch for O(1) expiry range checks.
-- Instead of scanning 6 months of touches and building arrays, we do a direct lookup
-- to check if the next touch (if any) is after the expiry block.
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
-- Materialize the 6-month-ago block range once
old_block_range AS (
    SELECT
        min(block_number) as min_old_block,
        max(block_number) as max_old_block
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time >= (SELECT min_time - INTERVAL 6 MONTH - INTERVAL 1 DAY FROM current_bounds)
        AND block_date_time <= (SELECT max_time - INTERVAL 6 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- Candidate slots: SET in the 6-month-ago window
candidate_expiries AS (
    SELECT
        d.block_number as set_block,
        eb.block_date_time as set_time,
        d.address,
        d.slot_key,
        d.effective_bytes_to as effective_bytes
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }} d FINAL
    INNER JOIN {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} eb FINAL
        ON d.block_number = eb.block_number
    WHERE d.block_number BETWEEN (SELECT min_old_block FROM old_block_range) AND (SELECT max_old_block FROM old_block_range)
        AND d.effective_bytes_to > 0
        AND eb.block_date_time >= (SELECT min_time - INTERVAL 6 MONTH - INTERVAL 1 DAY FROM current_bounds)
        AND eb.block_date_time <= (SELECT max_time - INTERVAL 6 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- Build expiry threshold mapping: for each block in bounds, calculate the 6-month-ago threshold
expiry_thresholds AS (
    SELECT
        block_number as expiry_block,
        block_date_time - INTERVAL 6 MONTH as threshold_time,
        lagInFrame(block_date_time - INTERVAL 6 MONTH) OVER (ORDER BY block_number) as prev_threshold_time
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Map candidates to their expiry block
candidates_with_expiry AS (
    SELECT
        c.set_block,
        c.address,
        c.slot_key,
        c.effective_bytes,
        e.expiry_block
    FROM candidate_expiries c
    INNER JOIN expiry_thresholds e
        ON c.set_time <= e.threshold_time
        AND (e.prev_threshold_time IS NULL OR c.set_time > e.prev_threshold_time)
)
SELECT
    now() as updated_date_time,
    c.expiry_block as block_number,
    c.address,
    c.slot_key,
    c.effective_bytes
FROM candidates_with_expiry c
-- Direct lookup: get next_touch_block for each candidate's set_block
INNER JOIN {{ index .dep "{{transformation}}" "int_storage_slot_next_touch" "helpers" "from" }} n FINAL
    ON c.address = n.address AND c.slot_key = n.slot_key AND c.set_block = n.block_number
WHERE
    -- Guard: only process if current bounds are 6+ months after Ethereum genesis
    (SELECT min_time FROM current_bounds) >= toDateTime('2016-01-30 00:00:00')
    -- Expiry block must fall within current bounds
    AND c.expiry_block BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    -- Only expire if no touch occurred between set and expiry:
    -- next_touch_block IS NULL means never touched again, OR
    -- next_touch_block > expiry_block means next touch is after expiry window
    AND (n.next_touch_block IS NULL OR n.next_touch_block > c.expiry_block)
