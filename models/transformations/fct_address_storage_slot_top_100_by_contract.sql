---
table: fct_address_storage_slot_top_100_by_contract
type: scheduled
schedule: "@every 2m"
tags:
  - address
  - storage
  - active
  - top100
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  row_number() OVER (ORDER BY total_storage_slots DESC, address ASC) as rank,
  address AS contract_address,
  count(*) AS total_storage_slots
FROM `{{ .self.database }}`.`int_address_storage_slot_last_access` FINAL
GROUP BY address
ORDER BY rank ASC
LIMIT 100;
