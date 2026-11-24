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
-- Keeps successful call frames that carry ETH from a watched address to an
-- address outside the list. delegate_call frames are left out: they show the
-- caller's msg.value in the trace but move no ETH.
--
-- Forward fill follows the head of the traces. Backfill walks back from there in
-- 10,000-block intervals and stops at limits.min, which is below the first block
-- with ETH sent from these addresses.
--
-- Traces are read without FINAL. Duplicate trace rows collapse in this
-- ReplacingMergeTree, whose sorting key ends with the frame's identity.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
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
    AND (error IS NULL OR error = '')
    AND action_to NOT IN (
        '0x12d66f87a04a9e220743712ce6d9bb1b5616b8fc',
        '0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936',
        '0x910cbd523d972eb0a6f4cae4618ad62622b39dbf',
        '0xa160cdab225685da1d56aa342ad8841c3b53f291',
        '0x722122df12d4e14e13ac3b6895a86e84145b6967',
        '0xd90e2f925da726b50c4ed8d0fb90ad053324f31b'
    )
