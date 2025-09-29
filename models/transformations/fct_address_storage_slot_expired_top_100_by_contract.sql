---
table: fct_address_storage_slot_expired_top_100_by_contract
interval:
  max: 86400
  min: 86400
schedules:
  forwardfill: "@every 1m"
tags:
  - address
  - storage
  - expired
  - top100
dependencies:
  - "{{transformation}}.fct_block"
  # TODO: should be added with scheduled transformations
  # - "{{transformation}}.int_address_storage_slot_last_access"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH latest_block AS (
    SELECT max(slot_start_date_time) as slot_start_date_time
    FROM `{{ index .dep "{{transformation}}" "fct_block" "database" }}`.`fct_block` FINAL
    WHERE `status` = 'canonical'
),
block_range AS (
    -- Get the block range for last 365 days
    SELECT
        min(execution_payload_block_number) as min_block_number
    FROM `{{ index .dep "{{transformation}}" "fct_block" "database" }}`.`fct_block` FINAL
    WHERE `status` = 'canonical'
        AND execution_payload_block_number IS NOT NULL
        AND slot_start_date_time >= (SELECT slot_start_date_time - INTERVAL 365 DAY FROM latest_block)
)
SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  row_number() OVER (ORDER BY expired_slots DESC) as rank,
  address AS contract_address,
  count(*) AS expired_slots
FROM `{{ index .dep "{{transformation}}" "fct_block" "database" }}`.`int_address_storage_slot_last_access` FINAL
WHERE block_number < (SELECT min_block_number FROM block_range)
GROUP BY address
ORDER BY expired_slots DESC
LIMIT 100;
