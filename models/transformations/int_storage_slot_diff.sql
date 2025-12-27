---
table: int_storage_slot_diff
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
dependencies:
  - "{{external}}.canonical_execution_storage_diffs"
  - "{{transformation}}.int_storage_selfdestruct_diffs"
---
-- Aggregates raw storage diffs into per-slot, per-block records with effective bytes.
-- Unions canonical storage diffs with synthetic selfdestruct clearance diffs to correctly
-- track storage state when contracts are selfdestructed with storage_cleared=true.
INSERT INTO
`{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Union both sources: canonical storage diffs + synthetic selfdestruct clearances
all_storage_diffs AS (
    -- Canonical storage diffs from execution traces
    SELECT
        block_number,
        transaction_index,
        internal_index,
        address,
        slot as slot_key,
        from_value,
        to_value
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
    UNION ALL
    -- Synthetic storage diffs from selfdestructs that clear storage
    SELECT
        block_number,
        transaction_index,
        internal_index,
        address,
        slot as slot_key,
        from_value,
        to_value
    FROM {{ index .dep "{{transformation}}" "int_storage_selfdestruct_diffs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    address,
    slot_key,
    -- Effective bytes: (length after trimming leading zeros + 1) / 2
    toUInt8((length(trimLeft(substring(first_from_value, 3), '0')) + 1) / 2) as effective_bytes_from,
    toUInt8((length(trimLeft(substring(last_to_value, 3), '0')) + 1) / 2) as effective_bytes_to
FROM (
    SELECT
        block_number,
        address,
        slot_key,
        argMin(from_value, (transaction_index, internal_index)) as first_from_value,
        argMax(to_value, (transaction_index, internal_index)) as last_to_value
    FROM all_storage_diffs
    GROUP BY block_number, address, slot_key
)
WHERE NOT (effective_bytes_from = 0 AND effective_bytes_to = 0)
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;
