---
table: fct_rocketpool_validator
type: scheduled
schedule: "@every 1h"
tags:
  - validator
  - rocketpool
dependencies:
  - "{{transformation}}.int_rocketpool_minipool"
  - "{{external}}.canonical_beacon_validators_withdrawal_credentials"
  - "{{external}}.canonical_beacon_validators_pubkeys"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH minipools AS (
    SELECT
        minipool_address,
        node_operator,
        event_date_time AS minipool_created_date_time
    FROM {{ index .dep "{{transformation}}" "int_rocketpool_minipool" "helpers" "from" }} FINAL
    WHERE event_name = 'created'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    wc.validator_index,
    ifNull(pk.pubkey, '') AS pubkey,
    m.minipool_address,
    m.node_operator,
    m.minipool_created_date_time
FROM
(
    SELECT
        `index` AS validator_index,
        lower(concat('0x', substring(withdrawal_credentials, 27))) AS minipool_address
    FROM {{ index .dep "{{external}}" "canonical_beacon_validators_withdrawal_credentials" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
) AS wc
GLOBAL INNER JOIN minipools AS m ON wc.minipool_address = m.minipool_address
GLOBAL LEFT JOIN
(
    SELECT
        `index` AS validator_index,
        pubkey
    FROM {{ index .dep "{{external}}" "canonical_beacon_validators_pubkeys" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
) AS pk ON wc.validator_index = pk.validator_index;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
