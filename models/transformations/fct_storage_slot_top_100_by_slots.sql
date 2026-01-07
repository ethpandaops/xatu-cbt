---
table: fct_storage_slot_top_100_by_slots
type: scheduled
schedule: "@every 1h"
tags:
  - storage
  - top100
dependencies:
  - "{{transformation}}.int_storage_slot_state_by_address"
  - "{{transformation}}.int_storage_slot_state_with_expiry_by_address"
---
-- Get the top 100 contracts by raw active_slots, then show their values
-- under each expiry policy. Rank is always based on raw state (NULL expiry_policy).
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get top 100 by raw state
top_100_raw AS (
    SELECT
        row_number() OVER (ORDER BY active_slots DESC, address ASC) as rank,
        address as contract_address,
        active_slots,
        effective_bytes
    FROM (
        SELECT
            address,
            argMax(active_slots, block_number) as active_slots,
            argMax(effective_bytes, block_number) as effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_state_by_address" "helpers" "from" }} FINAL
        GROUP BY address
    )
    ORDER BY rank ASC
    LIMIT 100
),
-- Get expiry state for the top 100 contracts
expiry_state AS (
    SELECT
        address,
        expiry_policy,
        argMax(active_slots, block_number) as active_slots,
        argMax(effective_bytes, block_number) as effective_bytes
    FROM {{ index .dep "{{transformation}}" "int_storage_slot_state_with_expiry_by_address" "helpers" "from" }} FINAL
    WHERE address IN (SELECT contract_address FROM top_100_raw)
    GROUP BY address, expiry_policy
)
-- Insert raw state rows (NULL expiry_policy)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    NULL as expiry_policy,
    rank,
    contract_address,
    active_slots,
    effective_bytes,
    dim.owner_key,
    dim.account_owner,
    dim.contract_name,
    dim.factory_contract,
    dim.labels
FROM top_100_raw AS state
LEFT JOIN `{{ .self.database }}`.`dim_contract_owner` AS dim FINAL
    ON state.contract_address = dim.contract_address
UNION ALL
-- Insert expiry policy rows
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    e.expiry_policy,
    r.rank,
    r.contract_address,
    e.active_slots,
    e.effective_bytes,
    dim.owner_key,
    dim.account_owner,
    dim.contract_name,
    dim.factory_contract,
    dim.labels
FROM top_100_raw AS r
INNER JOIN expiry_state AS e ON r.contract_address = e.address
LEFT JOIN `{{ .self.database }}`.`dim_contract_owner` AS dim FINAL
    ON r.contract_address = dim.contract_address
ORDER BY expiry_policy, rank ASC;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
