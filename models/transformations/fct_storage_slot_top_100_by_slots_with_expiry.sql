---
table: fct_storage_slot_top_100_by_slots_with_expiry
type: scheduled
schedule: "@every 1h"
tags:
  - storage
  - top100
  - expiry
dependencies:
  - "{{transformation}}.int_storage_slot_state_with_expiry_by_address"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    expiry_policy,
    rank,
    contract_address,
    active_slots,
    effective_bytes,
    dim.owner_key,
    dim.account_owner,
    dim.contract_name,
    dim.factory_contract,
    dim.usage_category
FROM (
    SELECT
        expiry_policy,
        row_number() OVER (PARTITION BY expiry_policy ORDER BY active_slots DESC, address ASC) as rank,
        address as contract_address,
        active_slots,
        effective_bytes
    FROM (
        SELECT
            address,
            expiry_policy,
            argMax(active_slots, block_number) as active_slots,
            argMax(effective_bytes, block_number) as effective_bytes
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_state_with_expiry_by_address" "helpers" "from" }} FINAL
        GROUP BY address, expiry_policy
    )
) AS state
LEFT JOIN `{{ .self.database }}`.`dim_contract_owner` AS dim FINAL
    ON state.contract_address = dim.contract_address
WHERE rank <= 100
ORDER BY expiry_policy, rank ASC;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
