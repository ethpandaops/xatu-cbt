---
table: fct_address_access_total
type: scheduled
schedule: "@every 2m"
tags:
  - address
  - access
  - total
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH latest_block AS (
    SELECT max(slot_start_date_time) as slot_start_date_time
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
total_contracts AS (
    SELECT COUNT(DISTINCT contract_address) AS count
    FROM `{{ .self.database }}`.`canonical_execution_contracts` FINAL
),
total_accounts AS (
    SELECT COUNT(*) AS count
    FROM `{{ .self.database }}`.`int_address_last_access` FINAL
),
expired_accounts AS (
    -- Expired accounts (not accessed in last 365 days)
    SELECT COUNT(*) AS count
    FROM `{{ .self.database }}`.`int_address_last_access` FINAL
    WHERE block_number < (SELECT min_block_number FROM block_range)
),
expired_contracts AS (
    -- Expired contracts (not accessed in last 365 days)
    SELECT COUNT(*) AS count
    FROM `{{ .self.database }}`.`int_address_last_access` AS a FINAL
    GLOBAL INNER JOIN (
    SELECT DISTINCT lower(contract_address) AS contract_address
    FROM `{{ .self.database }}`.`canonical_execution_contracts` FINAL
    ) AS c
    ON a.address = c.contract_address
    WHERE a.block_number < (SELECT min_block_number FROM block_range)
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    (SELECT count FROM total_accounts) AS total_accounts,
    (SELECT count FROM expired_accounts) AS expired_accounts,
    (SELECT count FROM total_contracts) AS total_contracts,
    (SELECT count FROM expired_contracts) AS expired_contracts;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
