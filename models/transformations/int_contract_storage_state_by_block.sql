---
table: int_contract_storage_state_by_block
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
  - "{{transformation}}.int_contract_storage_state"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get the last known cumulative state before this chunk
prev_state AS (
    SELECT
        active_slots,
        effective_bytes,
        active_contracts
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE block_number < {{ .bounds.start }}
    ORDER BY block_number DESC
    LIMIT 1
),
-- Get the previous active_slots per address at the start of this chunk
-- This is needed to detect contract activation/deactivation transitions
prev_address_state AS (
    SELECT
        address,
        argMax(active_slots, block_number) as prev_active_slots
    FROM {{ index .dep "{{transformation}}" "int_contract_storage_state" "helpers" "from" }} FINAL
    WHERE block_number < {{ .bounds.start }}
    GROUP BY address
),
-- Aggregate address-level deltas to block-level (sparse - only blocks with changes)
-- Also calculate contracts_delta based on contract activation/deactivation
sparse_deltas AS (
    SELECT
        s.block_number,
        toInt32(SUM(s.slots_delta)) as slots_delta,
        SUM(s.bytes_delta) as bytes_delta,
        -- Count contracts that became active (prev<=0, now>0) or inactive (prev>0, now<=0)
        toInt32(SUM(CASE
            WHEN s.active_slots > 0 AND COALESCE(p.prev_active_slots, 0) <= 0 THEN 1
            WHEN s.active_slots <= 0 AND COALESCE(p.prev_active_slots, 0) > 0 THEN -1
            ELSE 0
        END)) as contracts_delta
    FROM {{ index .dep "{{transformation}}" "int_contract_storage_state" "helpers" "from" }} s FINAL
    LEFT JOIN prev_address_state p ON s.address = p.address
    WHERE s.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY s.block_number
),
-- Generate all block numbers in the range
all_blocks AS (
    SELECT toUInt32({{ .bounds.start }} + number) as block_number
    FROM numbers(toUInt64({{ .bounds.end }} - {{ .bounds.start }} + 1))
),
-- Join to get deltas for all blocks (0 for blocks with no changes)
block_deltas AS (
    SELECT
        a.block_number,
        COALESCE(d.slots_delta, 0) as slots_delta,
        COALESCE(d.bytes_delta, 0) as bytes_delta,
        COALESCE(d.contracts_delta, 0) as contracts_delta
    FROM all_blocks a
    LEFT JOIN sparse_deltas d ON a.block_number = d.block_number
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    slots_delta,
    bytes_delta,
    contracts_delta,
    COALESCE((SELECT active_slots FROM prev_state), 0)
        + SUM(slots_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING) as active_slots,
    COALESCE((SELECT effective_bytes FROM prev_state), 0)
        + SUM(bytes_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING) as effective_bytes,
    COALESCE((SELECT active_contracts FROM prev_state), 0)
        + SUM(contracts_delta) OVER (ORDER BY block_number ROWS UNBOUNDED PRECEDING) as active_contracts
FROM block_deltas
ORDER BY block_number
SETTINGS
    join_algorithm = 'hash',
    max_threads = 4;
