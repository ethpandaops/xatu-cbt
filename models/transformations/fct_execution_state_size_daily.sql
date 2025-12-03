---
table: fct_execution_state_size_daily
type: incremental
interval:
  type: block
  max: 100000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 1m"
tags:
  - daily
  - execution
  - state_size
dependencies:
  - "{{transformation}}.fct_execution_state_size_hourly"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    toDate(hour_start_date_time) AS date,
    count(*) AS hour_count,
    min(min_block_number) AS min_block_number,
    max(max_block_number) AS max_block_number,
    argMax(accounts, hour_start_date_time) AS accounts,
    argMax(account_bytes, hour_start_date_time) AS account_bytes,
    argMax(account_trienodes, hour_start_date_time) AS account_trienodes,
    argMax(account_trienode_bytes, hour_start_date_time) AS account_trienode_bytes,
    argMax(contract_codes, hour_start_date_time) AS contract_codes,
    argMax(contract_code_bytes, hour_start_date_time) AS contract_code_bytes,
    argMax(storages, hour_start_date_time) AS storages,
    argMax(storage_bytes, hour_start_date_time) AS storage_bytes,
    argMax(storage_trienodes, hour_start_date_time) AS storage_trienodes,
    argMax(storage_trienode_bytes, hour_start_date_time) AS storage_trienode_bytes,
    sum(accounts_delta) AS accounts_delta,
    sum(account_bytes_delta) AS account_bytes_delta,
    sum(storage_bytes_delta) AS storage_bytes_delta,
    sum(contract_code_bytes_delta) AS contract_code_bytes_delta,
    argMax(total_bytes, hour_start_date_time) AS total_bytes
FROM {{ index .dep "{{transformation}}" "fct_execution_state_size_hourly" "helpers" "from" }} FINAL
WHERE max_block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY toDate(hour_start_date_time)