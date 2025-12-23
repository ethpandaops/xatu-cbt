---
table: int_contract_storage_state_with_expiry_by_block
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
  - contract
  - cumulative
dependencies:
  - "{{transformation}}.int_contract_storage_state_with_expiry"
---
-- Network-wide aggregation of contract-level expiry state per block.
-- Computes cumulative state for each policy: 1m, 6m, 12m, 18m, 24m.
-- Uses contract-level expiry semantics: a contract's slots only expire when
-- NO slot of that contract has been touched for the expiry period (all-or-nothing).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Previous cumulative state from self
prev_state AS (
    SELECT
        ps_expiry_policy,
        active_slots as prev_active_slots,
        effective_bytes as prev_effective_bytes,
        active_contracts as prev_active_contracts
    FROM (
        SELECT
            expiry_policy as ps_expiry_policy,
            active_slots,
            effective_bytes,
            active_contracts,
            row_number() OVER (PARTITION BY expiry_policy ORDER BY block_number DESC) as rn
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE block_number < {{ .bounds.start }}
    )
    WHERE rn = 1
),
-- Deltas from contract-level per-address table
-- Aggregates changes across all addresses for each (block, policy)
block_deltas AS (
    SELECT
        block_number,
        expiry_policy,
        SUM(active_slots - prev_active_slots) as slots_delta,
        SUM(effective_bytes - prev_effective_bytes) as bytes_delta,
        SUM(CASE
            WHEN active_slots > 0 AND prev_active_slots <= 0 THEN 1
            WHEN active_slots <= 0 AND prev_active_slots > 0 THEN -1
            ELSE 0
        END) as contracts_delta
    FROM {{ index .dep "{{transformation}}" "int_contract_storage_state_with_expiry" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY block_number, expiry_policy
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    d.block_number,
    d.expiry_policy,
    COALESCE(p.prev_active_slots, 0)
        + SUM(d.slots_delta) OVER (
            PARTITION BY d.expiry_policy
            ORDER BY d.block_number
            ROWS UNBOUNDED PRECEDING
        ) as active_slots,
    COALESCE(p.prev_effective_bytes, 0)
        + SUM(d.bytes_delta) OVER (
            PARTITION BY d.expiry_policy
            ORDER BY d.block_number
            ROWS UNBOUNDED PRECEDING
        ) as effective_bytes,
    COALESCE(p.prev_active_contracts, 0)
        + SUM(d.contracts_delta) OVER (
            PARTITION BY d.expiry_policy
            ORDER BY d.block_number
            ROWS UNBOUNDED PRECEDING
        ) as active_contracts
FROM block_deltas d
LEFT JOIN prev_state p ON d.expiry_policy = p.ps_expiry_policy
ORDER BY d.expiry_policy, d.block_number;
