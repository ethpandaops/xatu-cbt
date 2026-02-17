---
table: int_block_resource_gas
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
  - resource
  - block
dependencies:
  - "{{transformation}}.int_transaction_resource_gas"
---
-- Per-block resource gas totals.
--
-- Simple aggregation of int_transaction_resource_gas grouped by block_number.
-- Useful for block-level resource consumption analysis (e.g., "this block was
-- memory-heavy" or "this block had high state growth").
--
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    sum(gas_compute) AS gas_compute,
    sum(gas_memory) AS gas_memory,
    sum(gas_address_access) AS gas_address_access,
    sum(gas_state_growth) AS gas_state_growth,
    sum(gas_history) AS gas_history,
    sum(gas_bloom_topics) AS gas_bloom_topics,
    sum(gas_block_size) AS gas_block_size,
    sum(gas_refund) AS gas_refund,
    meta_network_name
FROM {{ index .dep "{{transformation}}" "int_transaction_resource_gas" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY
    block_number,
    meta_network_name
