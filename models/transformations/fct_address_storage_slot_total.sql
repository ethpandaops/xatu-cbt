---
table: fct_address_storage_slot_total
type: scheduled
schedule: "@every 24h"
tags:
  - address
  - storage
  - total
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH latest_block AS (
    SELECT max(slot_start_date_time) AS slot_start_date_time
    FROM `{{ .self.database }}`.`fct_block` FINAL
    WHERE `status` = 'canonical'
),
block_range AS (
    -- Get the block range for last 365 days
    SELECT
        max(execution_payload_block_number) AS max_block_number,
        min(execution_payload_block_number) AS min_block_number
    FROM `{{ .self.database }}`.`fct_block` FINAL
    WHERE `status` = 'canonical'
        AND execution_payload_block_number IS NOT NULL
        AND slot_start_date_time >= (SELECT slot_start_date_time - INTERVAL 365 DAY FROM latest_block)
),
total_storage_slots AS (
    SELECT COUNT(*) AS count
    FROM `{{ .self.database }}`.`int_address_storage_slot_last_access` FINAL
),
expired_storage_slots AS (
    -- Expired storage slots (not accessed in last 365 days)
    SELECT COUNT(*) AS count
    FROM `{{ .self.database }}`.`int_address_storage_slot_last_access` FINAL
    WHERE block_number < (SELECT min_block_number FROM block_range)
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    (SELECT count FROM total_storage_slots) as total_storage_slots,
    (SELECT count FROM expired_storage_slots) as expired_storage_slots;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
