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
        address,
        creation_block
    FROM {{ index .dep "{{transformation}}" "int_contract_selfdestruct" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND storage_cleared = true
),
-- For same-batch multiple SDs of same address: find the previous SD that cleared storage
-- This handles the case where contract is selfdestructed, recreated, and selfdestructed again in same batch
previous_sd_in_batch AS (
    SELECT
        sd.block_number,
        sd.transaction_index,
        sd.internal_index,
        sd.address,
        -- Find the most recent previous SD of same address in this batch
        argMax(
            (prev.block_number, prev.transaction_index, prev.internal_index),
            (prev.block_number, prev.transaction_index, prev.internal_index)
        ) as prev_sd
    FROM clearing_selfdestructs sd
    INNER JOIN clearing_selfdestructs prev
        ON sd.address = prev.address
        AND (prev.block_number, prev.transaction_index, prev.internal_index)
            < (sd.block_number, sd.transaction_index, sd.internal_index)
    GROUP BY sd.block_number, sd.transaction_index, sd.internal_index, sd.address
),
-- Minimum creation block across all selfdestructs (for bounded historical scan)
-- This prevents full table scan from block 0
min_creation_block AS (
    SELECT coalesce(min(creation_block), {{ .bounds.start }}) as min_block
    FROM clearing_selfdestructs
),
-- Union canonical storage diffs with previous synthetic diffs (Bug 2 fix)
-- BOUNDED: canonical uses creation_block to bounds.end, synthetic uses block < bounds.start
all_storage_history AS (
    SELECT block_number, transaction_index, internal_index, address, slot, to_value
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }}
    WHERE address GLOBAL IN (SELECT address FROM clearing_selfdestructs)
        AND block_number >= (SELECT min_block FROM min_creation_block)  -- Lower bound from creation
        AND block_number <= {{ .bounds.end }}  -- Safe upper bound (SDs are within bounds)
        AND meta_network_name = '{{ .env.NETWORK }}'
    UNION ALL
    SELECT block_number, transaction_index, internal_index, address, slot, to_value
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE address GLOBAL IN (SELECT address FROM clearing_selfdestructs)
        AND block_number < {{ .bounds.start }}  -- Only previous runs' output
),
-- Get the last value for each slot BEFORE each selfdestruct (Bug 1 fix)
-- Uses per-selfdestruct filtering instead of global bounds.start
-- For same-batch multiple SDs: only look at storage AFTER the previous SD (which cleared everything)
all_slots_before_selfdestruct AS (
    SELECT
        sd.block_number as sd_block,
        sd.transaction_index as sd_tx_index,
        sd.internal_index as sd_internal_index,
        sd.transaction_hash,
        sd.address,
        d.slot,
        argMax(d.to_value, (d.block_number, d.transaction_index, d.internal_index)) as last_value
    FROM clearing_selfdestructs sd
    LEFT JOIN previous_sd_in_batch prev
        ON sd.block_number = prev.block_number
        AND sd.transaction_index = prev.transaction_index
        AND sd.internal_index = prev.internal_index
        AND sd.address = prev.address
    INNER JOIN all_storage_history d
        ON sd.address = d.address
        -- Storage must be before this selfdestruct
        AND (d.block_number, d.transaction_index, d.internal_index)
            < (sd.block_number, sd.transaction_index, sd.internal_index)
        -- If there was a previous SD in this batch, only consider storage AFTER it
        -- (previous SD cleared everything, so only new storage since then matters)
        AND (prev.prev_sd IS NULL
             OR (d.block_number, d.transaction_index, d.internal_index) > prev.prev_sd)
    GROUP BY sd.block_number, sd.transaction_index, sd.internal_index, sd.transaction_hash, sd.address, d.slot
    HAVING last_value != '0x0000000000000000000000000000000000000000000000000000000000000000'
)
-- Final SELECT: Generate synthetic diffs
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    sd_block as block_number,
    sd_tx_index as transaction_index,
    transaction_hash,
    sd_internal_index as internal_index,
    address,
    slot,
    last_value as from_value,
    -- to_value: Always zero (cleared)
    '0x0000000000000000000000000000000000000000000000000000000000000000' as to_value
FROM all_slots_before_selfdestruct
ORDER BY block_number, transaction_index, internal_index, slot
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;
