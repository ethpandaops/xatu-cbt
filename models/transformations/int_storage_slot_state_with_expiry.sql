---
table: int_storage_slot_state_with_expiry
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
  - "{{transformation}}.int_storage_slot_state"
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
-- Unified storage slot state with all expiry policies per address.
-- Computes cumulative state for each policy: 1m, 6m, 12m, 18m, 24m.
-- Layers expiry/reactivation deltas on top of base int_storage_slot_state.
--
-- IMPORTANT: Addresses can appear here from TWO sources:
-- 1. Base activity (slot reads/writes) from int_storage_slot_state
-- 2. Expiry/reactivation events (even without new slot activity)
-- Both must be included to correctly track expiry effects.
--
-- Note: Uses aliased column names in subqueries to avoid ClickHouse correlated column resolution.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Expiry deltas per address per policy (NEGATIVE)
expiry_deltas AS (
    SELECT
        ed_block_number,
        ed_address,
        ed_expiry_policy,
        -toInt64(count()) as slots_delta,
        -SUM(toInt64(effective_bytes)) as bytes_delta
    FROM (
        SELECT block_number as ed_block_number, address as ed_address, '1m' as ed_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_1m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number as ed_block_number, address as ed_address, '6m' as ed_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_6m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number as ed_block_number, address as ed_address, '12m' as ed_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_12m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number as ed_block_number, address as ed_address, '18m' as ed_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_18m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number as ed_block_number, address as ed_address, '24m' as ed_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_24m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    )
    GROUP BY ed_block_number, ed_address, ed_expiry_policy
),
-- Reactivation deltas per address per policy (POSITIVE)
reactivation_deltas AS (
    SELECT
        rd_block_number,
        rd_address,
        rd_expiry_policy,
        toInt64(count()) as slots_delta,
        SUM(toInt64(effective_bytes)) as bytes_delta
    FROM (
        SELECT block_number as rd_block_number, address as rd_address, '1m' as rd_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_1m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number as rd_block_number, address as rd_address, '6m' as rd_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_6m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number as rd_block_number, address as rd_address, '12m' as rd_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_12m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number as rd_block_number, address as rd_address, '18m' as rd_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_18m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        UNION ALL
        SELECT block_number as rd_block_number, address as rd_address, '24m' as rd_expiry_policy, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_24m" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    )
    GROUP BY rd_block_number, rd_address, rd_expiry_policy
),
-- Unique (block, address, policy) from expiry/reactivation events
expiry_reactivation_keys AS (
    SELECT DISTINCT block_number, address, expiry_policy
    FROM (
        SELECT ed_block_number as block_number, ed_address as address, ed_expiry_policy as expiry_policy
        FROM expiry_deltas
        UNION ALL
        SELECT rd_block_number, rd_address, rd_expiry_policy
        FROM reactivation_deltas
    )
),
-- Base activity from int_storage_slot_state
base_activity AS (
    SELECT
        block_number,
        address,
        active_slots as base_active_slots,
        effective_bytes as base_effective_bytes
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_state" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Unique addresses from expiry/reactivation events (for filtering prev_base_state)
expiry_reactivation_addresses AS (
    SELECT DISTINCT address FROM expiry_reactivation_keys
),
-- Previous base state for addresses that have expiry/reactivation but no base activity in this range
-- Use INNER JOIN instead of GLOBAL IN (ClickHouse analyzer doesn't support CTE refs in GLOBAL IN)
prev_base_state AS (
    SELECT
        address,
        prev_base_active_slots,
        prev_base_effective_bytes
    FROM (
        SELECT
            s.address,
            s.block_number,
            s.active_slots as prev_base_active_slots,
            s.effective_bytes as prev_base_effective_bytes,
            row_number() OVER (PARTITION BY s.address ORDER BY s.block_number DESC) as rn
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_state" "helpers" "from" }} s FINAL
        INNER JOIN expiry_reactivation_addresses k ON s.address = k.address
        WHERE s.block_number < {{ .bounds.start }}
    )
    WHERE rn = 1
),
-- Combined: all (block, address, policy) pairs that need processing
-- Source 1: Base activity cross-joined with all policies
-- Source 2: Expiry/reactivation events (even without base activity)
all_block_addresses AS (
    -- Base activity (has slot reads/writes)
    SELECT
        b.block_number,
        b.address,
        p.expiry_policy,
        b.base_active_slots,
        b.base_effective_bytes
    FROM base_activity b
    CROSS JOIN (SELECT arrayJoin(['1m', '6m', '12m', '18m', '24m']) as expiry_policy) p

    UNION ALL

    -- Expiry/reactivation only (no base activity in this block)
    -- Use previous base state
    SELECT
        e.block_number,
        e.address,
        e.expiry_policy,
        COALESCE(p.prev_base_active_slots, toInt64(0)) as base_active_slots,
        COALESCE(p.prev_base_effective_bytes, toInt64(0)) as base_effective_bytes
    FROM expiry_reactivation_keys e
    LEFT JOIN prev_base_state p ON e.address = p.address
    LEFT JOIN base_activity b ON e.block_number = b.block_number AND e.address = b.address
    WHERE b.address = ''  -- ClickHouse uses '' not NULL for unmatched String columns
),
-- Join deltas to all_block_addresses
with_deltas AS (
    SELECT
        a.block_number,
        a.address,
        a.expiry_policy,
        a.base_active_slots,
        a.base_effective_bytes,
        COALESCE(ed.slots_delta, toInt64(0)) + COALESCE(rd.slots_delta, toInt64(0)) as net_slots_delta,
        COALESCE(ed.bytes_delta, toInt64(0)) + COALESCE(rd.bytes_delta, toInt64(0)) as net_bytes_delta
    FROM all_block_addresses a
    LEFT JOIN expiry_deltas ed ON a.block_number = ed.ed_block_number AND a.address = ed.ed_address AND a.expiry_policy = ed.ed_expiry_policy
    LEFT JOIN reactivation_deltas rd ON a.block_number = rd.rd_block_number AND a.address = rd.rd_address AND a.expiry_policy = rd.rd_expiry_policy
),
-- Previous cumulative net state per address per policy from self
prev_cumulative_state AS (
    SELECT
        ps_address,
        ps_expiry_policy,
        cumulative_net_slots as prev_cumulative_net_slots,
        cumulative_net_bytes as prev_cumulative_net_bytes
    FROM (
        SELECT
            address as ps_address,
            expiry_policy as ps_expiry_policy,
            cumulative_net_slots,
            cumulative_net_bytes,
            row_number() OVER (PARTITION BY address, expiry_policy ORDER BY block_number DESC) as rn
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE block_number < {{ .bounds.start }}
    )
    WHERE rn = 1
),
-- Join with prev_cumulative_state
joined AS (
    SELECT
        w.block_number,
        w.address,
        w.expiry_policy,
        w.base_active_slots,
        w.base_effective_bytes,
        w.net_slots_delta,
        w.net_bytes_delta,
        COALESCE(ps.prev_cumulative_net_slots, toInt64(0)) as prev_cumulative_net_slots,
        COALESCE(ps.prev_cumulative_net_bytes, toInt64(0)) as prev_cumulative_net_bytes
    FROM with_deltas w
    LEFT JOIN prev_cumulative_state ps ON w.address = ps.ps_address AND w.expiry_policy = ps.ps_expiry_policy
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    address,
    expiry_policy,
    net_slots_delta,
    net_bytes_delta,
    prev_cumulative_net_slots
        + SUM(net_slots_delta) OVER (PARTITION BY address, expiry_policy ORDER BY block_number ROWS UNBOUNDED PRECEDING) as cumulative_net_slots,
    prev_cumulative_net_bytes
        + SUM(net_bytes_delta) OVER (PARTITION BY address, expiry_policy ORDER BY block_number ROWS UNBOUNDED PRECEDING) as cumulative_net_bytes,
    base_active_slots + (
        prev_cumulative_net_slots
        + SUM(net_slots_delta) OVER (PARTITION BY address, expiry_policy ORDER BY block_number ROWS UNBOUNDED PRECEDING)
    ) as active_slots,
    base_effective_bytes + (
        prev_cumulative_net_bytes
        + SUM(net_bytes_delta) OVER (PARTITION BY address, expiry_policy ORDER BY block_number ROWS UNBOUNDED PRECEDING)
    ) as effective_bytes
FROM joined
ORDER BY expiry_policy, block_number, address;
