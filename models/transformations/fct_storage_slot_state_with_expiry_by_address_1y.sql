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
-- Get base state changes for this chunk (from non-expiry state table)
base_deltas AS (
    SELECT
        address,
        max(last_block_number) as last_block_number,
        -- These are already cumulative in the source, so we need the latest values
        argMax(active_slots, last_block_number) as base_active_slots,
        argMax(effective_bytes, last_block_number) as base_effective_bytes
    FROM {{ index .dep "{{transformation}}" "fct_storage_slot_state_by_address" "helpers" "from" }} FINAL
    WHERE last_block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY address
),
-- Get expiry deltas for this chunk, aggregated per address
expiry_deltas AS (
    SELECT
        address,
        max(block_number) as last_expiry_block,
        SUM(expiry_slots_delta) as expiry_slots_delta,
        SUM(expiry_bytes_delta) as expiry_bytes_delta
    FROM {{ index .dep "{{transformation}}" "fct_storage_slot_expiry_delta_by_address_1y" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY address
),
-- Union of addresses that had either base changes or expiries
all_addresses AS (
    SELECT address FROM base_deltas
    UNION DISTINCT
    SELECT address FROM expiry_deltas
),
-- Get previous state for all affected addresses
prev_state AS (
    SELECT
        address,
        cumulative_expiry_slots,
        cumulative_expiry_bytes,
        active_slots as prev_active_slots,
        effective_bytes as prev_effective_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE address IN (SELECT address FROM all_addresses)
),
-- Get current base state for addresses that only had expiries (no base changes in this chunk)
current_base_state AS (
    SELECT
        address,
        active_slots as base_active_slots,
        effective_bytes as base_effective_bytes
    FROM {{ index .dep "{{transformation}}" "fct_storage_slot_state_by_address" "helpers" "from" }} FINAL
    WHERE address IN (SELECT address FROM expiry_deltas WHERE address NOT IN (SELECT address FROM base_deltas))
)
SELECT
    now() as updated_date_time,
    a.address,
    greatest(
        COALESCE(b.last_block_number, 0),
        COALESCE(e.last_expiry_block, 0)
    ) as last_block_number,
    COALESCE(p.cumulative_expiry_slots, 0) + COALESCE(e.expiry_slots_delta, 0) as cumulative_expiry_slots,
    COALESCE(p.cumulative_expiry_bytes, 0) + COALESCE(e.expiry_bytes_delta, 0) as cumulative_expiry_bytes,
    -- active_slots = base_active_slots + cumulative_expiry_slots (expiry is negative)
    COALESCE(b.base_active_slots, cbs.base_active_slots, p.prev_active_slots - p.cumulative_expiry_slots, 0)
        + COALESCE(p.cumulative_expiry_slots, 0) + COALESCE(e.expiry_slots_delta, 0) as active_slots,
    COALESCE(b.base_effective_bytes, cbs.base_effective_bytes, p.prev_effective_bytes - p.cumulative_expiry_bytes, 0)
        + COALESCE(p.cumulative_expiry_bytes, 0) + COALESCE(e.expiry_bytes_delta, 0) as effective_bytes
FROM all_addresses a
LEFT JOIN base_deltas b ON a.address = b.address
LEFT JOIN expiry_deltas e ON a.address = e.address
LEFT JOIN prev_state p ON a.address = p.address
LEFT JOIN current_base_state cbs ON a.address = cbs.address
