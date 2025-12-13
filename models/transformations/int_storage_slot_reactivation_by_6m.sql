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
-- Tracks storage slot reactivations: slots touched after being expired for 6+ months.
-- Uses next_touch_block pointer to find reactivation candidates efficiently.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get timestamp for end of current bounds
current_bounds AS (
    SELECT max(block_date_time) as max_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Max block that is 6+ months before current bounds
old_block_range AS (
    SELECT max(block_number) as max_old_block
    FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
    WHERE block_date_time <= (SELECT max_time - INTERVAL 6 MONTH + INTERVAL 1 DAY FROM current_bounds)
),
-- Reactivation candidates: previous touch was > 6 months ago
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
-- Join with expiry table to get effective_bytes from the expiry record
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    r.reactivation_block as block_number,
    r.address,
    r.slot_key,
    argMax(e.effective_bytes, e.block_number) as effective_bytes
FROM reactivation_candidates r
INNER JOIN {{ index .dep "{{transformation}}" "int_storage_slot_expiry_by_6m" "helpers" "from" }} e FINAL
    ON r.address = e.address
    AND r.slot_key = e.slot_key
    AND e.block_number < r.reactivation_block
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
