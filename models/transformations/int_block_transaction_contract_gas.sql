---
table: int_block_transaction_contract_gas
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 10s"
  backfill: "@every 30s"
tags:
  - execution
  - gas
  - block
dependencies:
  - "{{transformation}}.int_transaction_call_frame"
---
-- Gas each contract used within each transaction of a block, the flows from transactions to contracts.
-- gas is the frames' own gas, excluding their children, so contracts add up without double counting. Root frames
-- count as a CALL to the transaction's target. Frames without a target or a call type are left out, and so are
-- contracts that used no gas (calls to accounts without code).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    transaction_hash,
    target AS target_address,
    any(transaction_index) AS transaction_index,
    toUInt32(count()) AS call_count,
    sum(gas) AS gas
FROM (
    SELECT
        block_number,
        transaction_hash,
        transaction_index,
        call_frame_id,
        assumeNotNull(target_address) AS target,
        if(call_type = '' AND call_frame_id = 0, 'CALL', call_type) AS frame_call_type,
        gas
    FROM {{ index .dep "{{transformation}}" "int_transaction_call_frame" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND target_address IS NOT NULL
        AND (call_type != '' OR call_frame_id = 0)
)
GROUP BY block_number, transaction_hash, target
HAVING gas > 0
