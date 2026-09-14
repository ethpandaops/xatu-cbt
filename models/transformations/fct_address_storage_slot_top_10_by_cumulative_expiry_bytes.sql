---
table: fct_address_storage_slot_top_10_by_cumulative_expiry_bytes
type: scheduled
schedule: "@every 1m"
tags:
  - address
  - storage
  - expiry
  - bytes
  - top10
dependencies:
  - "{{transformation}}.fct_storage_slot_state_with_expiry_by_address_1y"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  row_number() OVER (ORDER BY abs(cumulative_expiry_bytes) DESC, address ASC) as rank,
  address,
  cumulative_expiry_slots,
  cumulative_expiry_bytes,
  active_slots,
  effective_bytes
FROM {{ index .dep "{{transformation}}" "fct_storage_slot_state_with_expiry_by_address_1y" "helpers" "from" }} FINAL
ORDER BY rank ASC
LIMIT 10;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
