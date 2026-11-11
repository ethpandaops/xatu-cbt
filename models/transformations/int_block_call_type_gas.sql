---
table: int_block_call_type_gas
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
-- Internal calls per call type in a block: every frame except the transaction's root frame (call_frame_id 0).
-- gas is the frames' own gas, excluding their children, so the call types add up without double counting.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    call_type,
    toUInt32(count()) AS call_count,
    sum(gas) AS gas
FROM {{ index .dep "{{transformation}}" "int_transaction_call_frame" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND call_frame_id != 0
    AND call_type != ''
GROUP BY block_number, call_type
