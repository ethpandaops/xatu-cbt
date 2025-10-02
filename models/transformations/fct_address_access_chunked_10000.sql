---
table: fct_address_access_chunked_10000
interval:
  max: 86400
  min: 86400
schedules:
  forwardfill: "@every 2m"
tags:
  - address
  - access
dependencies:
  - "{{transformation}}.fct_block"
  # TODO: should be added with scheduled transformations
  # - "{{transformation}}.int_address_last_access"
  # - "{{transformation}}.int_address_first_access"
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
  FROM `{{ index .dep "{{transformation}}" "fct_block" "database" }}`.`int_address_first_access` FINAL

  UNION ALL

  SELECT
    intDiv(block_number, window_size) * window_size AS chunk_start_block_number,
    0 AS first_accessed,
    1 AS last_accessed
  FROM `{{ index .dep "{{transformation}}" "fct_block" "database" }}`.`int_address_last_access` FINAL
)
GROUP BY chunk_start_block_number
ORDER BY chunk_start_block_number;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});