---
table: fct_storage_slot_state_by_address
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
  - cumulative
dependencies:
  - "{{transformation}}.int_storage_slot_diff"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get list of addresses that have activity in this chunk
active_addresses AS (
    SELECT DISTINCT address
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Get the last known cumulative state before this chunk for each active address
prev_state AS (
    SELECT
        address,
        argMax(active_slots, block_number) as active_slots,
        argMax(effective_bytes, block_number) as effective_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE block_number < {{ .bounds.start }}
      AND address IN (SELECT address FROM active_addresses)
    GROUP BY address
),
-- Calculate deltas per block per address from storage slot diffs (sparse - only blocks with changes)
sparse_deltas AS (
    SELECT
        block_number,
        address,
        -- Slots activated: from=0, to>0 (+1)
        -- Slots deactivated: from>0, to=0 (-1)
        -- Slots modified: from>0, to>0 (no change)
        toInt64(countIf(effective_bytes_from = 0 AND effective_bytes_to > 0))
          - toInt64(countIf(effective_bytes_from > 0 AND effective_bytes_to = 0)) as slots_delta,
        -- Net byte change: sum of (to - from) for all changes
        SUM(toInt64(effective_bytes_to) - toInt64(effective_bytes_from)) as bytes_delta
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY block_number, address
)
-- Generate all block numbers in the range
all_blocks AS (
    SELECT toUInt32({{ .bounds.start }} + number) as block_number
    FROM numbers(toUInt64({{ .bounds.end }} - {{ .bounds.start }} + 1))
),
-- Join to get deltas for all blocks (0 for blocks with no changes)
block_deltas AS (
    SELECT
        a.block_number,
        a.address,
        COALESCE(d.slots_delta, 0) as slots_delta,
        COALESCE(d.bytes_delta, 0) as bytes_delta
    FROM all_blocks a
    LEFT JOIN sparse_deltas d ON a.block_number = d.block_number
)
SELECT
    now() as updated_date_time,
    block_number,
    address,
    slots_delta,
    bytes_delta,
    COALESCE((SELECT active_slots FROM prev_state p WHERE p.address = block_deltas.address), 0)
        + SUM(slots_delta) OVER (PARTITION BY address ORDER BY block_number ROWS UNBOUNDED PRECEDING) as active_slots,
    COALESCE((SELECT effective_bytes FROM prev_state p WHERE p.address = block_deltas.address), 0)
        + SUM(bytes_delta) OVER (PARTITION BY address ORDER BY block_number ROWS UNBOUNDED PRECEDING) as effective_bytes
FROM block_deltas
ORDER BY address, block_number