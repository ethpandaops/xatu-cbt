---
table: fct_storage_slot_state_with_expiry
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
  - "{{transformation}}.fct_storage_slot_state"
  - "{{transformation}}.int_storage_slot_expiry_1m"
  - "{{transformation}}.int_storage_slot_expiry_6m"
  - "{{transformation}}.int_storage_slot_expiry_12m"
  - "{{transformation}}.int_storage_slot_expiry_18m"
  - "{{transformation}}.int_storage_slot_expiry_24m"
  - "{{transformation}}.int_storage_slot_reactivation_1m"
  - "{{transformation}}.int_storage_slot_reactivation_6m"
  - "{{transformation}}.int_storage_slot_reactivation_12m"
  - "{{transformation}}.int_storage_slot_reactivation_18m"
  - "{{transformation}}.int_storage_slot_reactivation_24m"
---
-- Unified storage slot state with all expiry policies.
-- Computes cumulative state for each policy: 1m, 6m, 12m, 18m, 24m.
-- Layers expiry/reactivation deltas on top of base fct_storage_slot_state.
-- Note: Uses aliased column names in subqueries to avoid ClickHouse correlated column resolution.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    expiry_policy,
    net_slots_delta,
    net_bytes_delta,
    prev_cumulative_net_slots
        + SUM(net_slots_delta) OVER (PARTITION BY expiry_policy ORDER BY block_number ROWS UNBOUNDED PRECEDING) as cumulative_net_slots,
    prev_cumulative_net_bytes
        + SUM(net_bytes_delta) OVER (PARTITION BY expiry_policy ORDER BY block_number ROWS UNBOUNDED PRECEDING) as cumulative_net_bytes,
    base_active_slots + (
        prev_cumulative_net_slots
        + SUM(net_slots_delta) OVER (PARTITION BY expiry_policy ORDER BY block_number ROWS UNBOUNDED PRECEDING)
    ) as active_slots,
    base_effective_bytes + (
        prev_cumulative_net_bytes
        + SUM(net_bytes_delta) OVER (PARTITION BY expiry_policy ORDER BY block_number ROWS UNBOUNDED PRECEDING)
    ) as effective_bytes
FROM (
    -- Base stats cross-joined with policies, with prev_state and deltas
    SELECT
        bp.block_number,
        bp.expiry_policy,
        bp.base_active_slots,
        bp.base_effective_bytes,
        COALESCE(ed.slots_delta, 0) + COALESCE(rd.slots_delta, 0) as net_slots_delta,
        COALESCE(ed.bytes_delta, 0) + COALESCE(rd.bytes_delta, 0) as net_bytes_delta,
        COALESCE(ps.prev_cumulative_net_slots, toInt64(0)) as prev_cumulative_net_slots,
        COALESCE(ps.prev_cumulative_net_bytes, toInt64(0)) as prev_cumulative_net_bytes
    FROM (
        -- Base stats cross-joined with policies
        SELECT
            b.block_number,
            p.expiry_policy,
            b.base_active_slots,
            b.base_effective_bytes
        FROM (
            SELECT
                block_number,
                active_slots as base_active_slots,
                effective_bytes as base_effective_bytes
            FROM {{ index .dep "{{transformation}}" "fct_storage_slot_state" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        ) b
        CROSS JOIN (SELECT arrayJoin(['1m', '6m', '12m', '18m', '24m']) as expiry_policy) p
    ) bp
    LEFT JOIN (
        -- Expiry deltas per policy (NEGATIVE) - uses ed_ prefix to avoid correlated column issues
        SELECT
            ed_block_number,
            ed_expiry_policy,
            -toInt64(count()) as slots_delta,
            -SUM(toInt64(effective_bytes)) as bytes_delta
        FROM (
            SELECT block_number as ed_block_number, '1m' as ed_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_1m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            UNION ALL
            SELECT block_number as ed_block_number, '6m' as ed_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_6m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            UNION ALL
            SELECT block_number as ed_block_number, '12m' as ed_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_12m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            UNION ALL
            SELECT block_number as ed_block_number, '18m' as ed_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_18m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            UNION ALL
            SELECT block_number as ed_block_number, '24m' as ed_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_24m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        )
        GROUP BY ed_block_number, ed_expiry_policy
    ) ed ON bp.block_number = ed.ed_block_number AND bp.expiry_policy = ed.ed_expiry_policy
    LEFT JOIN (
        -- Reactivation deltas per policy (POSITIVE) - uses rd_ prefix to avoid correlated column issues
        SELECT
            rd_block_number,
            rd_expiry_policy,
            toInt64(count()) as slots_delta,
            SUM(toInt64(effective_bytes)) as bytes_delta
        FROM (
            SELECT block_number as rd_block_number, '1m' as rd_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_1m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            UNION ALL
            SELECT block_number as rd_block_number, '6m' as rd_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_6m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            UNION ALL
            SELECT block_number as rd_block_number, '12m' as rd_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_12m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            UNION ALL
            SELECT block_number as rd_block_number, '18m' as rd_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_18m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            UNION ALL
            SELECT block_number as rd_block_number, '24m' as rd_expiry_policy, effective_bytes
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_24m" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        )
        GROUP BY rd_block_number, rd_expiry_policy
    ) rd ON bp.block_number = rd.rd_block_number AND bp.expiry_policy = rd.rd_expiry_policy
    LEFT JOIN (
        -- Previous state per policy - uses ps_ prefix to avoid correlated column issues
        SELECT
            ps_expiry_policy,
            cumulative_net_slots as prev_cumulative_net_slots,
            cumulative_net_bytes as prev_cumulative_net_bytes
        FROM (
            SELECT
                expiry_policy as ps_expiry_policy,
                cumulative_net_slots,
                cumulative_net_bytes,
                row_number() OVER (PARTITION BY expiry_policy ORDER BY block_number DESC) as rn
            FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
            WHERE block_number < {{ .bounds.start }}
        )
        WHERE rn = 1
    ) ps ON bp.expiry_policy = ps.ps_expiry_policy
)
ORDER BY expiry_policy, block_number;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }})
AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }};
