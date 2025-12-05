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
  - "{{transformation}}.fct_storage_slot_delta_by_address"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get deltas for this chunk, aggregated per address
chunk_deltas AS (
    SELECT
        address,
        max(block_number) as last_block_number,
        SUM(slots_delta) as slots_delta,
        SUM(bytes_delta) as bytes_delta
    FROM {{ index .dep "{{transformation}}" "fct_storage_slot_delta_by_address" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY address
),
-- Get previous state for addresses in this chunk
prev_state AS (
    SELECT
        address,
        active_slots,
        effective_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE address IN (SELECT address FROM chunk_deltas)
)
SELECT
    now() as updated_date_time,
    d.address,
    d.last_block_number,
    COALESCE(p.active_slots, 0) + d.slots_delta as active_slots,
    COALESCE(p.effective_bytes, 0) + d.bytes_delta as effective_bytes
FROM chunk_deltas d
LEFT JOIN prev_state p ON d.address = p.address
