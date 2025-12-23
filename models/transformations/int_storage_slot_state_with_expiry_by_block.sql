---
table: int_storage_slot_state_with_expiry_by_block
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
  - "{{transformation}}.int_storage_slot_state_by_block"
  - "{{transformation}}.int_storage_slot_state_with_expiry"
---
-- Aggregated storage slot state with all expiry policies per block.
-- Computes cumulative state for each policy: 1m, 6m, 12m, 18m, 24m.
-- Layers expiry/reactivation deltas (aggregated from int_storage_slot_state_with_expiry)
-- on top of base int_storage_slot_state_by_block.
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
    SELECT
        bp.block_number,
        bp.expiry_policy,
        bp.base_active_slots,
        bp.base_effective_bytes,
        COALESCE(bd.net_slots_delta, 0) as net_slots_delta,
        COALESCE(bd.net_bytes_delta, 0) as net_bytes_delta,
        COALESCE(ps.prev_cumulative_net_slots, toInt64(0)) as prev_cumulative_net_slots,
        COALESCE(ps.prev_cumulative_net_bytes, toInt64(0)) as prev_cumulative_net_bytes
    FROM (
        -- Base stats from int_storage_slot_state_by_block cross-joined with policies
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
            FROM {{ index .dep "{{transformation}}" "int_storage_slot_state_by_block" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        ) b
        CROSS JOIN (SELECT arrayJoin(['1m', '6m', '12m', '18m', '24m']) as expiry_policy) p
    ) bp
    LEFT JOIN (
        -- Aggregate net deltas from int_storage_slot_state_with_expiry per block per policy
        SELECT
            block_number as bd_block_number,
            expiry_policy as bd_expiry_policy,
            toInt32(SUM(net_slots_delta)) as net_slots_delta,
            SUM(net_bytes_delta) as net_bytes_delta
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_state_with_expiry" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        GROUP BY block_number, expiry_policy
    ) bd ON bp.block_number = bd.bd_block_number AND bp.expiry_policy = bd.bd_expiry_policy
    LEFT JOIN (
        -- Previous state per policy
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
