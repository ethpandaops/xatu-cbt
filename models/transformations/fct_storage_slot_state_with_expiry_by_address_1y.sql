---
table: fct_storage_slot_state_with_expiry_by_address_1y
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
  - cumulative
dependencies:
  - "{{transformation}}.fct_storage_slot_state_by_address"
  - "{{transformation}}.fct_storage_slot_expiry_delta_by_address_1y"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get expiry deltas for this chunk, aggregated per address
expiry_deltas AS (
    SELECT
        address,
        SUM(expiry_slots_delta) as expiry_slots_delta,
        SUM(expiry_bytes_delta) as expiry_bytes_delta
    FROM {{ index .dep "{{transformation}}" "fct_storage_slot_expiry_delta_by_address_1y" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY address
),
-- Get current base state for addresses that had expiries
-- fct_storage_slot_state_by_address has single row per address with latest cumulative values
base_state AS (
    SELECT
        address,
        active_slots as base_active_slots,
        effective_bytes as base_effective_bytes
    FROM {{ index .dep "{{transformation}}" "fct_storage_slot_state_by_address" "helpers" "from" }} FINAL
    WHERE address IN (SELECT address FROM expiry_deltas)
),
-- Get previous expiry state for addresses in this chunk
prev_expiry_state AS (
    SELECT
        address,
        cumulative_expiry_slots,
        cumulative_expiry_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE address IN (SELECT address FROM expiry_deltas)
)
SELECT
    now() as updated_date_time,
    e.address,
    COALESCE(p.cumulative_expiry_slots, 0) + e.expiry_slots_delta as cumulative_expiry_slots,
    COALESCE(p.cumulative_expiry_bytes, 0) + e.expiry_bytes_delta as cumulative_expiry_bytes,
    -- active_slots = base_active_slots + new cumulative_expiry_slots (expiry is negative)
    COALESCE(b.base_active_slots, 0)
        + COALESCE(p.cumulative_expiry_slots, 0) + e.expiry_slots_delta as active_slots,
    COALESCE(b.base_effective_bytes, 0)
        + COALESCE(p.cumulative_expiry_bytes, 0) + e.expiry_bytes_delta as effective_bytes
FROM expiry_deltas e
LEFT JOIN base_state b ON e.address = b.address
LEFT JOIN prev_expiry_state p ON e.address = p.address
