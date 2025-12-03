---
table: fct_execution_state_size_hourly
type: incremental
interval:
  type: block
  max: 100000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 1m"
tags:
  - hourly
  - execution
  - state_size
dependencies:
  - "{{external}}.execution_state_size"
  - "{{external}}.canonical_execution_block"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    toStartOfHour(b.block_date_time) AS hour_start_date_time,
    count(*) AS measurement_count,
    min(s.block_number) AS min_block_number,
    max(s.block_number) AS max_block_number,
    argMax(s.accounts, s.block_number) AS accounts,
    argMax(s.account_bytes, s.block_number) AS account_bytes,
    argMax(s.account_trienodes, s.block_number) AS account_trienodes,
    argMax(s.account_trienode_bytes, s.block_number) AS account_trienode_bytes,
    argMax(s.contract_codes, s.block_number) AS contract_codes,
    argMax(s.contract_code_bytes, s.block_number) AS contract_code_bytes,
    argMax(s.storages, s.block_number) AS storages,
    argMax(s.storage_bytes, s.block_number) AS storage_bytes,
    argMax(s.storage_trienodes, s.block_number) AS storage_trienodes,
    argMax(s.storage_trienode_bytes, s.block_number) AS storage_trienode_bytes,
    toInt64(argMax(s.accounts, s.block_number)) - toInt64(argMin(s.accounts, s.block_number)) AS accounts_delta,
    toInt64(argMax(s.account_bytes, s.block_number)) - toInt64(argMin(s.account_bytes, s.block_number)) AS account_bytes_delta,
    toInt64(argMax(s.storage_bytes, s.block_number)) - toInt64(argMin(s.storage_bytes, s.block_number)) AS storage_bytes_delta,
    toInt64(argMax(s.contract_code_bytes, s.block_number)) - toInt64(argMin(s.contract_code_bytes, s.block_number)) AS contract_code_bytes_delta,
    argMax(s.account_trienode_bytes, s.block_number) 
        + argMax(s.contract_code_bytes, s.block_number) 
        + argMax(s.storage_trienode_bytes, s.block_number) AS total_bytes
FROM {{ index .dep "{{external}}" "execution_state_size" "helpers" "from" }} AS s FINAL
GLOBAL INNER JOIN {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} AS b FINAL
    ON s.block_number = b.block_number
WHERE s.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY toStartOfHour(b.block_date_time)