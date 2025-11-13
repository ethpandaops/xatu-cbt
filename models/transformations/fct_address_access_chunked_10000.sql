---
table: fct_address_access_chunked_10000
type: scheduled
schedule: "@every 24h"
tags:
  - address
  - access
dependencies:
  - "{{transformation}}.int_address_first_access"
  - "{{transformation}}.int_address_last_access"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
  10000 AS window_size
SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  chunk_start_block_number,
  sum(first_accessed) AS first_accessed_accounts,
  sum(last_accessed) AS last_accessed_accounts
FROM (
  SELECT
    intDiv(block_number, window_size) * window_size AS chunk_start_block_number,
    1 AS first_accessed,
    0 AS last_accessed
  FROM {{ index .dep "{{transformation}}" "int_address_first_access" "helpers" "from" }} FINAL

  UNION ALL

  SELECT
    intDiv(block_number, window_size) * window_size AS chunk_start_block_number,
    0 AS first_accessed,
    1 AS last_accessed
  FROM {{ index .dep "{{transformation}}" "int_address_last_access" "helpers" "from" }} FINAL
)
GROUP BY chunk_start_block_number
ORDER BY chunk_start_block_number;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
