---
table: int_storage_slot_diff
type: incremental
interval:
  type: block
  max: 100000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1s"
tags:
  - execution
  - storage
dependencies:
  - "{{external}}.canonical_execution_storage_diffs"
---
-- Transforms raw storage diffs into per-slot, per-block aggregated records.
-- For each (address, slot_key, block_number), stores the effective bytes before/after.
--
-- Architecture:
-- - Main table (int_storage_slot_diff): Full history, ORDER BY (address, slot_key, block_number)
-- - Helper table (int_storage_slot_diff_latest_state): Latest diff per slot, ORDER BY (address, slot_key)
-- - This model writes to BOTH tables atomically for consistency
--
INSERT INTO
`{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    address,
    slot_key,
    -- Calculate effective bytes from hex strings:
    -- '0x00...00ff' -> 1 byte, '0x<32 bytes>' -> 32 bytes, '0x00...00' -> 0 bytes
    -- Formula: (length of hex string after trimming leading zeros + 1) / 2
    toUInt8((length(trimLeft(substring(first_from_value, 3), '0')) + 1) / 2) as effective_bytes_from,
    toUInt8((length(trimLeft(substring(last_to_value, 3), '0')) + 1) / 2) as effective_bytes_to
FROM (
    -- Aggregate first, then compute effective bytes in outer query
    -- This is ~4x faster than computing string operations inside argMin/argMax
    SELECT
        block_number,
        address,
        slot as slot_key,
        -- Get FIRST from_value in block (min transaction_index, internal_index)
        -- Tuple comparison is faster than combined UInt64 key multiplication
        argMin(from_value, (transaction_index, internal_index)) as first_from_value,
        -- Get LAST to_value in block (max transaction_index, internal_index)
        argMax(to_value, (transaction_index, internal_index)) as last_to_value
    -- FINAL is not needed as GROUP BY handles deduplication and source has no actual duplicates
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY block_number, address, slot
)
WHERE NOT (effective_bytes_from = 0 AND effective_bytes_to = 0)
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;

-- INSERT 2: Update diff_latest_state helper table
-- Query from the data we just inserted in INSERT 1 (avoids re-querying external source).
-- ReplacingMergeTree(updated_date_time) will deduplicate by (address, slot_key).
INSERT INTO `{{ .self.database }}`.int_storage_slot_diff_latest_state
SELECT
    updated_date_time,
    address,
    slot_key,
    latest_block_number as block_number,
    effective_bytes_to
FROM (
    SELECT
        max(updated_date_time) as updated_date_time,
        address,
        slot_key,
        argMax(block_number, block_number) as latest_block_number,
        argMax(effective_bytes_to, block_number) as effective_bytes_to
    FROM `{{ .self.database }}`.`{{ .self.table }}`
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    GROUP BY address, slot_key
);
