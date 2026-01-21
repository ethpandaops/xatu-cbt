---
table: int_storage_slot_state
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
  - cumulative
dependencies:
  - "{{transformation}}.int_storage_slot_diff"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get the last known cumulative state per address before this chunk
prev_state AS (
    SELECT
        address,
        argMax(active_slots, block_number) as active_slots,
        argMax(effective_bytes, block_number) as effective_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE block_number < {{ .bounds.start }}
    GROUP BY address
),
-- Calculate deltas per block per address from storage slot diffs
address_deltas AS (
    SELECT
        block_number,
        address,
        -- Slots activated: from=0, to>0 (+1)
        -- Slots deactivated: from>0, to=0 (-1)
        -- Slots modified: from>0, to>0 (no change)
        toInt32(countIf(effective_bytes_from = 0 AND effective_bytes_to > 0))
          - toInt32(countIf(effective_bytes_from > 0 AND effective_bytes_to = 0)) as slots_delta,
        -- Net byte change: sum of (to - from) for all changes
        SUM(toInt64(effective_bytes_to) - toInt64(effective_bytes_from)) as bytes_delta
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY block_number, address
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    address,
    slots_delta,
    bytes_delta,
    COALESCE(p.active_slots, 0)
        + SUM(slots_delta) OVER (PARTITION BY d.address ORDER BY block_number ROWS UNBOUNDED PRECEDING) as active_slots,
    COALESCE(p.effective_bytes, 0)
        + SUM(bytes_delta) OVER (PARTITION BY d.address ORDER BY block_number ROWS UNBOUNDED PRECEDING) as effective_bytes
FROM address_deltas d
LEFT JOIN prev_state p ON d.address = p.address
ORDER BY block_number, address
SETTINGS
    join_algorithm = 'hash',
    max_threads = 4;
