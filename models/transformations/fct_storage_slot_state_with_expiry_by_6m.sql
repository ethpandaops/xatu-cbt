---
table: fct_storage_slot_state_with_expiry_by_6m
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
  - "{{transformation}}.fct_storage_slot_state"
  - "{{transformation}}.int_storage_slot_expiry_by_6m"
---
-- Layers 6-month expiry policy on top of fct_storage_slot_state: slots not accessed (read or written) for 6 months are cleared.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get the last known cumulative expiry totals before this chunk
prev_state AS (
    SELECT
        cumulative_expiry_slots,
        cumulative_expiry_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE block_number < {{ .bounds.start }}
    ORDER BY block_number DESC
    LIMIT 1
),
-- Base stats from fct_storage_slot_state (without expiry)
base_stats AS (
    SELECT
        block_number,
        active_slots as base_active_slots,
        effective_bytes as base_effective_bytes
    FROM {{ index .dep "{{transformation}}" "fct_storage_slot_state" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Expiry deltas: slots cleared due to 6-month inactivity
expiry_deltas AS (
    SELECT
        block_number,
        -toInt64(count()) as expiry_slots_delta,
        -SUM(toInt64(effective_bytes)) as expiry_bytes_delta
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_by_6m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY block_number
),
-- Join base stats with expiry deltas
combined AS (
    SELECT
        b.block_number,
        b.base_active_slots,
        b.base_effective_bytes,
        COALESCE(e.expiry_slots_delta, 0) as expiry_slots_delta,
        COALESCE(e.expiry_bytes_delta, 0) as expiry_bytes_delta
    FROM base_stats b
    LEFT JOIN expiry_deltas e ON b.block_number = e.block_number
)
SELECT
    now() as updated_date_time,
    block_number,
    expiry_slots_delta,
    expiry_bytes_delta,
    COALESCE((SELECT cumulative_expiry_slots FROM prev_state), 0)
        + SUM(expiry_slots_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING) as cumulative_expiry_slots,
    COALESCE((SELECT cumulative_expiry_bytes FROM prev_state), 0)
        + SUM(expiry_bytes_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING) as cumulative_expiry_bytes,
    base_active_slots + (
        COALESCE((SELECT cumulative_expiry_slots FROM prev_state), 0)
        + SUM(expiry_slots_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING)
    ) as active_slots,
    base_effective_bytes + (
        COALESCE((SELECT cumulative_expiry_bytes FROM prev_state), 0)
        + SUM(expiry_bytes_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING)
    ) as effective_bytes
FROM combined
ORDER BY block_number
