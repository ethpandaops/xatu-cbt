---
table: int_transaction_call_frame
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - execution
  - call
  - gas
dependencies:
  - "{{external}}.canonical_execution_transaction_structlog"
---
-- Aggregates structlog opcodes into per-call-frame records for transaction call tree analysis.
-- Each row represents one call frame within a transaction, with aggregated metrics.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    transaction_hash,
    transaction_index,
    call_frame_id,
    -- Parent frame is second-to-last in path, NULL for root frame
    if(length(call_frame_path) > 1,
       call_frame_path[length(call_frame_path) - 1],
       NULL) as parent_call_frame_id,
    -- Depth is path length - 1 (root is depth 0)
    toUInt32(length(call_frame_path) - 1) as depth,
    -- Get the target address from the CALL opcode that started this frame
    -- This is the address being called into
    anyIf(call_to_address, call_to_address IS NOT NULL AND call_to_address != '') as target_address,
    -- Call type is the opcode that started this frame (CALL, DELEGATECALL, etc.)
    anyIf(operation, operation IN ('CALL', 'DELEGATECALL', 'STATICCALL', 'CALLCODE', 'CREATE', 'CREATE2')) as call_type,
    -- Aggregate metrics for this frame
    count(*) as opcode_count,
    sum(gas_used) as total_gas,
    countIf(error IS NOT NULL AND error != '') as error_count
FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog" "helpers" "from" }}
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY
    block_number,
    transaction_hash,
    transaction_index,
    call_frame_id,
    call_frame_path
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;
