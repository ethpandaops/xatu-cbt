---
table: int_execution_block_by_date
type: incremental
interval:
  type: block
  max: 500000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5s"
tags:
  - execution
dependencies:
  - "{{external}}.canonical_execution_block"
  - "{{external}}.canonical_execution_transaction"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    b.block_date_time,
    b.block_number,
    -- Per-block transaction count, aggregated once here (a simple top-level model) so the
    -- cross-shard count() merges globally. Downstream models (e.g. fct_execution_tps_hourly) read
    -- this column instead of re-counting the transaction_hash-sharded canonical_execution_transaction
    -- inside a nested query, where the count would otherwise be evaluated shard-local and partial.
    toUInt32(COALESCE(t.transaction_count, 0)) AS transaction_count
FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} AS b FINAL
GLOBAL LEFT JOIN (
    SELECT
        block_number,
        count() AS transaction_count
    FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY block_number
) AS t ON b.block_number = t.block_number
WHERE b.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND b.meta_network_name = '{{ .env.NETWORK }}'
