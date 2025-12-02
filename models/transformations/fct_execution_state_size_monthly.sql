---
table: fct_execution_state_size_monthly
type: incremental
interval:
  type: block
  max: 50000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 5m"
tags:
  - monthly
  - execution
  - state_size
dependencies:
  - "{{transformation}}.fct_execution_state_size_daily"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    toStartOfMonth(date) AS month,
    count(*) AS day_count,
    min(min_block_number) AS min_block_number,
    max(max_block_number) AS max_block_number,
    argMax(accounts, date) AS accounts,
    argMax(account_bytes, date) AS account_bytes,
    argMax(account_trienodes, date) AS account_trienodes,
    argMax(account_trienode_bytes, date) AS account_trienode_bytes,
    argMax(contract_codes, date) AS contract_codes,
    argMax(contract_code_bytes, date) AS contract_code_bytes,
    argMax(storages, date) AS storages,
    argMax(storage_bytes, date) AS storage_bytes,
    argMax(storage_trienodes, date) AS storage_trienodes,
    argMax(storage_trienode_bytes, date) AS storage_trienode_bytes,
    sum(accounts_delta) AS accounts_delta,
    sum(account_bytes_delta) AS account_bytes_delta,
    sum(storage_bytes_delta) AS storage_bytes_delta,
    sum(contract_code_bytes_delta) AS contract_code_bytes_delta,
    argMax(total_bytes, date) AS total_bytes
FROM {{ index .dep "{{transformation}}" "fct_execution_state_size_daily" "helpers" "from" }} FINAL
WHERE max_block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY toStartOfMonth(date)
