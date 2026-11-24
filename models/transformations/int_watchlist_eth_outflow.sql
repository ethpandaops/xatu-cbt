---
table: int_watchlist_eth_outflow
type: incremental
interval:
  type: block
  max: 10000
fill:
  direction: "head"
  allow_gap_skipping: false
limits:
  min: 9100000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 30s"
tags:
  - execution
  - transfer
dependencies:
  - "{{external}}.canonical_execution_traces"
---
-- ETH sent out of a fixed list of mainnet addresses, one row per transfer.
--
-- Keeps plain ETH transfers: call frames with a value and no calldata, from a
-- watched address to an address outside the list. delegate_call frames are left
-- out: they show the caller's msg.value in the trace but move no ETH. Calls with
-- calldata are left out too: they are the watched addresses calling another
-- contract, such as forwarding a relayer's refund into a pool's withdraw, not a
-- payout.
--
-- A transfer only counts when the EVM kept it: its frame, every ancestor frame
-- and the transaction's root frame succeeded.
--
-- Forward fill follows the head of the traces. Backfill walks back from there in
-- 10,000-block intervals and stops at limits.min, which is below the first block
-- with ETH sent from these addresses.
--
-- Traces are read without FINAL. Duplicate trace rows collapse in this
-- ReplacingMergeTree, whose sorting key ends with the frame's identity.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
transfers AS (
    SELECT
        block_number,
        transaction_index,
        transaction_hash,
        internal_index,
        ifNull(trace_address, '') AS trace_address,
        action_from AS from_address,
        assumeNotNull(action_to) AS to_address,
        action_value AS value
    FROM {{ index .dep "{{external}}" "canonical_execution_traces" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
        -- The watched addresses are mainnet contracts, so other networks read nothing.
        AND '{{ .env.NETWORK }}' = 'mainnet'
        -- Watched addresses
        AND action_from IN (
            '0x12d66f87a04a9e220743712ce6d9bb1b5616b8fc',
            '0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936',
            '0x910cbd523d972eb0a6f4cae4618ad62622b39dbf',
            '0xa160cdab225685da1d56aa342ad8841c3b53f291',
            '0x722122df12d4e14e13ac3b6895a86e84145b6967',
            '0xd90e2f925da726b50c4ed8d0fb90ad053324f31b'
        )
        AND action_call_type = 'call'
        AND action_value > 0
        AND action_input IS NULL
        AND (error IS NULL OR error = '')
        AND action_to NOT IN (
            '0x12d66f87a04a9e220743712ce6d9bb1b5616b8fc',
            '0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936',
            '0x910cbd523d972eb0a6f4cae4618ad62622b39dbf',
            '0xa160cdab225685da1d56aa342ad8841c3b53f291',
            '0x722122df12d4e14e13ac3b6895a86e84145b6967',
            '0xd90e2f925da726b50c4ed8d0fb90ad053324f31b'
        )
),
-- Errored frames in the same transactions, as (internal_index, trace_address).
-- The root frame's trace_address is empty.
errored_frames AS (
    SELECT
        block_number,
        transaction_hash,
        groupArray((internal_index, ifNull(trace_address, ''))) AS frames
    FROM {{ index .dep "{{external}}" "canonical_execution_traces" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND error IS NOT NULL AND error != ''
        AND (block_number, transaction_hash) GLOBAL IN (
            SELECT block_number, transaction_hash FROM transfers
        )
    GROUP BY block_number, transaction_hash
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    t.block_number,
    t.transaction_index,
    t.transaction_hash,
    t.internal_index,
    t.trace_address,
    t.from_address,
    t.to_address,
    t.value
FROM transfers AS t
GLOBAL LEFT JOIN errored_frames AS e
    ON t.block_number = e.block_number
    AND t.transaction_hash = e.transaction_hash
-- Drop transfers under an errored root or ancestor frame, because the EVM
-- reverted them. An ancestor always comes before its descendants in
-- internal_index. Requiring that keeps stale rows from an earlier load of the
-- same transaction, which numbered its frames differently, from matching.
WHERE NOT arrayExists(
    f -> f.1 < t.internal_index
        AND (f.2 = '' OR startsWith(t.trace_address, concat(f.2, '_'))),
    e.frames
)
