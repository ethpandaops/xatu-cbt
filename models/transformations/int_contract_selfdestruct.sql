---
table: int_contract_selfdestruct
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
  - contract
  - selfdestruct
dependencies:
  - "{{external}}.canonical_execution_traces"
  - "{{transformation}}.int_contract_creation"
---
-- Tracks SELFDESTRUCT operations with EIP-6780 storage clearing implications.
-- Pre-Shanghai: SELFDESTRUCT always clears all storage slots.
-- Post-Shanghai (EIP-6780): SELFDESTRUCT only clears storage if contract was created in same tx.
-- SHANGHAI_BLOCK_NUMBER = 0 means the chain started in EIP-6780 era (post-Shanghai).
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Single-pass approach: read selfdestructs and root traces together, use window function
-- to determine if the transaction's root trace failed. This is more efficient than
-- GLOBAL NOT IN when there are many failed transactions (e.g., blocks 4.3M-4.5M have 1M+ failed txs).
-- Filter: selfdestructs OR root traces (to check for failures)
traces_with_tx_status AS (
    SELECT
        block_number,
        transaction_hash,
        transaction_index,
        internal_index,
        action_from as address,
        action_type,
        error,
        coalesce(action_to, '') as beneficiary,
        reinterpretAsUInt256(reverse(unhex(substring(action_value, 3)))) as value_transferred,
        -- Flag if this tx has a failed root trace (root = trace_address IS NULL or empty)
        maxIf(1, (trace_address IS NULL OR trace_address = '') AND error IS NOT NULL AND error != '')
            OVER (PARTITION BY block_number, transaction_hash) as tx_failed
    FROM {{ index .dep "{{external}}" "canonical_execution_traces" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
        -- Read both selfdestructs AND root traces (to check for tx failure)
        AND (action_type IN ('selfdestruct', 'suicide')
             OR ((trace_address IS NULL OR trace_address = '') AND error IS NOT NULL AND error != ''))
),
-- Filter to only successful selfdestructs in non-failed transactions
selfdestruct_traces AS (
    SELECT
        block_number,
        transaction_hash,
        transaction_index,
        internal_index,
        address,
        beneficiary,
        value_transferred
    FROM traces_with_tx_status
    WHERE action_type IN ('selfdestruct', 'suicide')
        AND (error IS NULL OR error = '')
        AND tx_failed = 0
),
-- Get contract creation info for addresses that were selfdestructed
-- Uses int_contract_creation with projection for efficient address lookups
-- Need every creation record (not grouped) to match same-block CREATE2 cycles correctly
-- Dedupe by (contract_address, block_number, transaction_hash, internal_index) using argMax
contract_creations AS (
    SELECT
        contract_address,
        block_number as creation_block,
        transaction_hash as creation_transaction_hash,
        transaction_index as creation_transaction_index,
        internal_index as creation_internal_index
    FROM {{ index .dep "{{transformation}}" "int_contract_creation" "helpers" "from" }}
    WHERE contract_address IN (SELECT address FROM selfdestruct_traces)
        -- Partition pruning: contracts can only be created before/at selfdestruct block
        AND block_number <= {{ .bounds.end }}
    GROUP BY contract_address, block_number, transaction_hash, transaction_index, internal_index
),
-- For each SELFDESTRUCT, find the most recent prior creation
-- Uses argMax with composite ordering to handle same-block CREATE2 cycles correctly:
-- - Same tx: use internal_index to order creation before selfdestruct
-- - Different tx in same block: creation must be in earlier tx (by transaction_index)
latest_creation AS (
    SELECT
        s.block_number,
        s.transaction_hash,
        s.transaction_index,
        s.internal_index,
        s.address,
        argMax(
            (c.creation_block, c.creation_transaction_hash),
            (c.creation_block,
             if(c.creation_block = s.block_number AND c.creation_transaction_hash = s.transaction_hash,
                c.creation_internal_index, 0))
        ).1 as creation_block,
        argMax(
            (c.creation_block, c.creation_transaction_hash),
            (c.creation_block,
             if(c.creation_block = s.block_number AND c.creation_transaction_hash = s.transaction_hash,
                c.creation_internal_index, 0))
        ).2 as creation_transaction_hash
    FROM selfdestruct_traces s
    LEFT JOIN contract_creations c
        ON s.address = c.contract_address
        AND (c.creation_block < s.block_number
             OR (c.creation_block = s.block_number
                 AND ((c.creation_transaction_hash = s.transaction_hash
                       AND c.creation_internal_index < s.internal_index)
                      OR (c.creation_transaction_hash != s.transaction_hash
                          AND c.creation_transaction_index < s.transaction_index))))
    GROUP BY s.block_number, s.transaction_hash, s.transaction_index, s.internal_index, s.address
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    s.block_number,
    s.transaction_hash,
    s.transaction_index,
    s.internal_index,
    s.address,
    s.beneficiary,
    s.value_transferred,
    -- ephemeral: true if contract was created and destroyed in same transaction
    (lc.creation_block = s.block_number AND lc.creation_transaction_hash = s.transaction_hash) as ephemeral,
    -- storage_cleared: pre-Shanghai always true, post-Shanghai only if ephemeral
    CASE
        WHEN {{ default "0" .env.SHANGHAI_BLOCK_NUMBER }} = 0 THEN
            -- Chain started in EIP-6780 era, only clear if ephemeral
            (lc.creation_block = s.block_number AND lc.creation_transaction_hash = s.transaction_hash)
        WHEN s.block_number < {{ default "0" .env.SHANGHAI_BLOCK_NUMBER }} THEN
            -- Pre-Shanghai: always clear storage
            true
        ELSE
            -- Post-Shanghai: only clear if ephemeral
            (lc.creation_block = s.block_number AND lc.creation_transaction_hash = s.transaction_hash)
    END as storage_cleared,
    lc.creation_block,
    lc.creation_transaction_hash
FROM selfdestruct_traces s
LEFT JOIN latest_creation lc
    ON s.block_number = lc.block_number
    AND s.transaction_hash = lc.transaction_hash
    AND s.transaction_index = lc.transaction_index
    AND s.internal_index = lc.internal_index
    AND s.address = lc.address
ORDER BY s.block_number, s.transaction_index, s.internal_index
SETTINGS
    -- Memory-safe join: spills to disk instead of OOM on large block ranges
    -- 128 buckets handles extreme cases like the 2.4M-2.5M spam period (37M selfdestructs)
    join_algorithm = 'grace_hash',
    grace_hash_join_initial_buckets = 128,
    max_bytes_before_external_group_by = 2000000000,
    max_bytes_before_external_sort = 2000000000,
    distributed_aggregation_memory_efficient = 1;
