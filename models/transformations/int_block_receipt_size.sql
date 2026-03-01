---
table: int_block_receipt_size
type: incremental
interval:
  type: block
  max: 10000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 10s"
  backfill: "@every 30s"
tags:
  - execution
  - receipt
  - size
  - block
dependencies:
  - "{{transformation}}.int_transaction_receipt_size"
---
-- Aggregates transaction receipt-size rows into exact per-block totals.
--
-- Derived from int_transaction_receipt_size by grouping all transactions in a block.
-- Includes per-transaction distribution stats within each block.
--
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    t.block_number,
    sum(t.receipt_bytes) AS receipt_bytes,
    toUInt32(count()) AS transaction_count,
    sum(t.log_count) AS log_count,
    sum(t.log_data_bytes) AS log_data_bytes,
    sum(t.log_topic_count) AS log_topic_count,
    round(avg(t.receipt_bytes), 4) AS avg_receipt_bytes_per_transaction,
    max(t.receipt_bytes) AS max_receipt_bytes_per_transaction,
    toUInt64(round(quantile(0.50)(t.receipt_bytes))) AS p50_receipt_bytes_per_transaction,
    toUInt64(round(quantile(0.95)(t.receipt_bytes))) AS p95_receipt_bytes_per_transaction,
    t.meta_network_name
FROM {{ index .dep "{{transformation}}" "int_transaction_receipt_size" "helpers" "from" }} AS t FINAL
WHERE t.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND t.meta_network_name = '{{ .env.NETWORK }}'
GROUP BY
    t.block_number,
    t.meta_network_name
