---
table: int_storage_selfdestruct_diffs
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5s"
tags:
  - execution
  - storage
  - selfdestruct
dependencies:
  - "{{transformation}}.int_contract_selfdestruct"
  - "{{external}}.canonical_execution_storage_diffs"
---
-- Generates synthetic storage diffs for SELFDESTRUCT operations that clear storage.
-- These synthetic diffs are unioned with canonical_execution_storage_diffs in
-- int_storage_slot_diff to correctly track storage state changes.
--
-- Only generates diffs for selfdestructs with storage_cleared=true:
--   - Pre-Shanghai: All selfdestructs
--   - Post-Shanghai (EIP-6780): Only ephemeral contracts
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get selfdestructs with storage_cleared=true in current bounds
clearing_selfdestructs AS (
    SELECT
        block_number,
        transaction_hash,
        transaction_index,
        internal_index,
        address
    FROM {{ index .dep "{{transformation}}" "int_contract_selfdestruct" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND storage_cleared = true
),
-- Get ALL historical slots for addresses with clearing selfdestructs
-- Gets the actual last known value for each slot before the current bounds
historical_slots AS (
    SELECT
        address,
        slot,
        argMax(to_value, (block_number, transaction_index, internal_index)) as last_value
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }}
    WHERE address GLOBAL IN (SELECT address FROM clearing_selfdestructs)
        AND block_number < {{ .bounds.start }}
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY address, slot
    -- Only include slots with non-zero final value (something to clear)
    HAVING last_value != '0x0000000000000000000000000000000000000000000000000000000000000000'
),
-- For current block slots, get the to_value just BEFORE the selfdestruct
-- Only needed for slots that have diffs BEFORE the selfdestruct (otherwise no clearance needed)
current_slots_before_selfdestruct AS (
    SELECT
        sd.block_number,
        sd.transaction_index as sd_tx_index,
        toUInt32(sd.internal_index) as sd_internal_index,
        sd.transaction_hash,
        sd.address,
        d.slot,
        argMax(d.to_value, (d.transaction_index, d.internal_index)) as value_before_selfdestruct
    FROM clearing_selfdestructs sd
    GLOBAL INNER JOIN {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }} d
        ON sd.block_number = d.block_number
        AND sd.address = d.address
        AND d.meta_network_name = '{{ .env.NETWORK }}'
        AND d.block_number <= {{ .bounds.end }}
    WHERE (d.transaction_index, d.internal_index) < (sd.transaction_index, sd.internal_index)
    GROUP BY sd.block_number, sd.transaction_index, sd.internal_index, sd.transaction_hash, sd.address, d.slot
    -- Only include if the value before selfdestruct was non-zero (otherwise 0x0 -> 0x0 is pointless)
    HAVING value_before_selfdestruct != '0x0000000000000000000000000000000000000000000000000000000000000000'
),
-- Historical slots that need clearance records
-- Each selfdestruct needs a clearance for every historical non-zero slot of that address
historical_clearances AS (
    SELECT
        sd.block_number,
        sd.transaction_hash,
        sd.transaction_index,
        toUInt32(sd.internal_index) as internal_index,
        sd.address,
        h.slot,
        h.last_value as from_value
    FROM clearing_selfdestructs sd
    INNER JOIN historical_slots h ON sd.address = h.address
),
-- Current block slots that need clearance (not already in historical)
current_clearances AS (
    SELECT
        block_number,
        transaction_hash,
        sd_tx_index as transaction_index,
        sd_internal_index as internal_index,
        address,
        slot,
        value_before_selfdestruct as from_value
    FROM current_slots_before_selfdestruct
    -- Exclude slots already covered by historical (avoid duplicates)
    WHERE (address, slot) NOT IN (SELECT address, slot FROM historical_slots)
),
-- Union: historical slots + current block slots with non-zero value before selfdestruct
slots_to_clear AS (
    SELECT * FROM historical_clearances
    UNION ALL
    SELECT * FROM current_clearances
)
-- Final SELECT: Generate synthetic diffs
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    transaction_index,
    transaction_hash,
    internal_index,
    address,
    slot,
    from_value,
    -- to_value: Always zero (cleared)
    '0x0000000000000000000000000000000000000000000000000000000000000000' as to_value
FROM slots_to_clear
ORDER BY block_number, transaction_index, internal_index, slot
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;
