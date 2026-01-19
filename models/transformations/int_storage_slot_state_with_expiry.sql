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
-- NOTE: Due to ClickHouse analyzer bug with multiple LEFT JOINs to separate CTEs
-- containing UNION ALL, we combine expiry and reactivation into a single CTE.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Combined expiry/reactivation events with signed values
-- Expiry = negative (slots/bytes removed), Reactivation = positive (slots/bytes restored)
all_delta_events AS (
    -- Expiry events (negative)
    SELECT block_number, address, '1m' as expiry_policy, -toInt64(1) as slots_delta, -toInt64(effective_bytes) as bytes_delta
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_1m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, '6m' as expiry_policy, -toInt64(1), -toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_6m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, '12m' as expiry_policy, -toInt64(1), -toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_12m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, '18m' as expiry_policy, -toInt64(1), -toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_18m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, '24m' as expiry_policy, -toInt64(1), -toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_24m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    -- Reactivation events (positive)
    SELECT block_number, address, '1m' as expiry_policy, toInt64(1), toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_1m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, '6m' as expiry_policy, toInt64(1), toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_6m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, '12m' as expiry_policy, toInt64(1), toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_12m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, '18m' as expiry_policy, toInt64(1), toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_18m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    UNION ALL
    SELECT block_number, address, '24m' as expiry_policy, toInt64(1), toInt64(effective_bytes)
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_reactivation_24m" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Aggregate all deltas per (block, address, policy)
combined_deltas AS (
    SELECT
        block_number,
        address,
        expiry_policy,
        SUM(slots_delta) as net_slots_delta,
        SUM(bytes_delta) as net_bytes_delta
    FROM all_delta_events
    GROUP BY block_number, address, expiry_policy
),
-- Unique (block, address, policy) from expiry/reactivation events
expiry_reactivation_keys AS (
    SELECT DISTINCT block_number, address, expiry_policy
    FROM combined_deltas
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
    -- Use ASOF JOIN to find the correct base state at each expiry block
    -- This fixes the bug where a single prev_base_state was used for all expiry blocks,
    -- ignoring base state changes within the processing batch
    SELECT
        e.block_number,
        e.address,
        e.expiry_policy,
        COALESCE(s.active_slots, toInt64(0)) as base_active_slots,
        COALESCE(s.effective_bytes, toInt64(0)) as base_effective_bytes
    FROM expiry_reactivation_keys e
    ASOF LEFT JOIN (
        SELECT block_number, address, active_slots, effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_state" "helpers" "from" }} FINAL
    ) s ON e.address = s.address AND e.block_number >= s.block_number
    LEFT JOIN base_activity b ON e.block_number = b.block_number AND e.address = b.address
    WHERE b.address = ''  -- ClickHouse uses '' not NULL for unmatched String columns
),
-- Join deltas to all_block_addresses
-- NOTE: Split into separate CTEs to work around ClickHouse analyzer bug with multiple LEFT JOINs
with_deltas AS (
    SELECT
        a.block_number,
        a.address,
        a.expiry_policy,
        a.base_active_slots,
        a.base_effective_bytes,
        COALESCE(d.net_slots_delta, toInt64(0)) as net_slots_delta,
        COALESCE(d.net_bytes_delta, toInt64(0)) as net_bytes_delta
    FROM all_block_addresses a
    LEFT JOIN combined_deltas d ON a.block_number = d.block_number AND a.address = d.address AND a.expiry_policy = d.expiry_policy
),
-- All unique addresses from current chunk (for filtering prev_cumulative_state)
all_active_addresses AS (
    SELECT DISTINCT address FROM with_deltas
),
-- Previous cumulative net state per address per policy from self
-- PERF: Use IN filter + argMax with tuple instead of FINAL for ~3x speedup and ~10x memory reduction
prev_cumulative_state AS (
    SELECT
        address as ps_address,
        expiry_policy as ps_expiry_policy,
        argMax(cumulative_net_slots, (block_number, updated_date_time)) as prev_cumulative_net_slots,
        argMax(cumulative_net_bytes, (block_number, updated_date_time)) as prev_cumulative_net_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}`
    WHERE address IN (SELECT address FROM all_active_addresses)
        AND block_number < {{ .bounds.start }}
    GROUP BY address, expiry_policy
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
