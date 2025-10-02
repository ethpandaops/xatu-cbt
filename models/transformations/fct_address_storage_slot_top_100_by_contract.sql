---
table: fct_address_storage_slot_top_100_by_contract
interval:
  max: 86400
  min: 86400
schedules:
  forwardfill: "@every 1m"
tags:
  - address
  - storage
  - active
  - top100
dependencies:
  - "{{transformation}}.fct_block"
  # TODO: should be added with scheduled transformations
  # - "{{transformation}}.int_address_storage_slot_last_access"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  row_number() OVER (ORDER BY total_storage_slots DESC, address ASC) as rank,
  address AS contract_address,
  count(*) AS total_storage_slots
FROM `{{ index .dep "{{transformation}}" "fct_block" "database" }}`.`int_address_storage_slot_last_access` FINAL
GROUP BY address
ORDER BY rank ASC
LIMIT 100;
