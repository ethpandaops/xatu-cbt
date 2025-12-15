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
  - "{{transformation}}.int_storage_slot_reactivation_by_6m"
---
-- Layers 6-month expiry policy on top of fct_storage_slot_state.
-- Combines expiry events (negative deltas) and reactivation events (positive deltas).
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get the last known cumulative net totals before this chunk
prev_state AS (
    SELECT
        cumulative_net_slots,
        cumulative_net_bytes
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
-- Expiry deltas: slots cleared due to 6-month inactivity (NEGATIVE)
expiry_deltas AS (
    SELECT
        block_number,
        -toInt64(count()) as slots_delta,
        -SUM(toInt64(effective_bytes)) as bytes_delta
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_by_6m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY block_number
),
-- Reactivation deltas: slots un-expired by subsequent touch (POSITIVE)
reactivation_deltas AS (
    SELECT
        block_number,
        toInt64(count()) as slots_delta,
        SUM(toInt64(effective_bytes)) as bytes_delta
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_by_6m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY block_number
),
-- Combine expiry and reactivation into net deltas per block
combined_deltas AS (
    SELECT
        block_number,
        SUM(slots_delta) as net_slots_delta,
        SUM(bytes_delta) as net_bytes_delta
    FROM (
        SELECT * FROM expiry_deltas
        UNION ALL
        SELECT * FROM reactivation_deltas
    )
    GROUP BY block_number
),
-- Join base stats with combined deltas
combined AS (
    SELECT
        b.block_number,
        b.base_active_slots,
        b.base_effective_bytes,
        COALESCE(d.net_slots_delta, 0) as net_slots_delta,
        COALESCE(d.net_bytes_delta, 0) as net_bytes_delta
    FROM base_stats b
    LEFT JOIN combined_deltas d ON b.block_number = d.block_number
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    net_slots_delta,
    net_bytes_delta,
    COALESCE((SELECT cumulative_net_slots FROM prev_state), 0)
        + SUM(net_slots_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING) as cumulative_net_slots,
    COALESCE((SELECT cumulative_net_bytes FROM prev_state), 0)
        + SUM(net_bytes_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING) as cumulative_net_bytes,
    base_active_slots + (
        COALESCE((SELECT cumulative_net_slots FROM prev_state), 0)
        + SUM(net_slots_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING)
    ) as active_slots,
    base_effective_bytes + (
        COALESCE((SELECT cumulative_net_bytes FROM prev_state), 0)
        + SUM(net_bytes_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING)
    ) as effective_bytes
FROM combined
ORDER BY block_number;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }})
AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }};
