---
table: fct_execution_state_size_monthly_delta
type: scheduled
schedule: "@every 5m"
tags:
  - monthly
  - execution
  - state_size
  - delta
dependencies:
  - "{{external}}.execution_state_size"
  - "{{external}}.canonical_execution_block"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    toStartOfMonth(b.block_date_time) AS month,
    toInt64(argMax(s.accounts, s.block_number)) - toInt64(argMin(s.accounts, s.block_number)) AS account_count_delta,
    toInt64(argMax(s.account_bytes, s.block_number)) - toInt64(argMin(s.account_bytes, s.block_number)) AS account_bytes_delta,
    toInt64(argMax(s.account_trienodes, s.block_number)) - toInt64(argMin(s.account_trienodes, s.block_number)) AS account_trienodes_delta,
    toInt64(argMax(s.account_trienode_bytes, s.block_number)) - toInt64(argMin(s.account_trienode_bytes, s.block_number)) AS account_trienode_bytes_delta,
    toInt64(argMax(s.contract_codes, s.block_number)) - toInt64(argMin(s.contract_codes, s.block_number)) AS contract_codes_delta,
    toInt64(argMax(s.contract_code_bytes, s.block_number)) - toInt64(argMin(s.contract_code_bytes, s.block_number)) AS contract_code_bytes_delta,
    toInt64(argMax(s.storages, s.block_number)) - toInt64(argMin(s.storages, s.block_number)) AS storage_count_delta,
    toInt64(argMax(s.storage_bytes, s.block_number)) - toInt64(argMin(s.storage_bytes, s.block_number)) AS storage_bytes_delta,
    toInt64(argMax(s.storage_trienodes, s.block_number)) - toInt64(argMin(s.storage_trienodes, s.block_number)) AS storage_trienodes_delta,
    toInt64(argMax(s.storage_trienode_bytes, s.block_number)) - toInt64(argMin(s.storage_trienode_bytes, s.block_number)) AS storage_trienode_bytes_delta,
    (toInt64(argMax(s.account_trienode_bytes, s.block_number)) + toInt64(argMax(s.contract_code_bytes, s.block_number)) + toInt64(argMax(s.storage_trienode_bytes, s.block_number)))
        - (toInt64(argMin(s.account_trienode_bytes, s.block_number)) + toInt64(argMin(s.contract_code_bytes, s.block_number)) + toInt64(argMin(s.storage_trienode_bytes, s.block_number))) AS total_bytes_delta
FROM {{ index .dep "{{external}}" "execution_state_size" "helpers" "from" }} AS s FINAL
GLOBAL INNER JOIN {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} AS b FINAL
    ON s.block_number = b.block_number
WHERE b.block_date_time >= now() - INTERVAL 24 MONTH
GROUP BY toStartOfMonth(b.block_date_time);

DELETE FROM `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }})
