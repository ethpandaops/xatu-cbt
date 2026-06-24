---
table: fct_rocketpool_validator
type: scheduled
schedule: "@every 1h"
tags:
  - validator
  - rocketpool
dependencies:
  - "{{transformation}}.int_rocketpool_minipool"
  - "{{transformation}}.int_rocketpool_megapool"
  - "{{external}}.canonical_beacon_validators_withdrawal_credentials"
  - "{{external}}.canonical_beacon_validators_pubkeys"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
minipools AS (
    SELECT minipool_address AS pool_address, node_operator, event_date_time AS created_date_time
    FROM {{ index .dep "{{transformation}}" "int_rocketpool_minipool" "helpers" "from" }} FINAL
    WHERE event_name = 'created'
),
megapools AS (
    SELECT megapool_address AS pool_address, node_operator, created_date_time
    FROM {{ index .dep "{{transformation}}" "int_rocketpool_megapool" "helpers" "from" }} FINAL
),
creds AS (
    SELECT
        `index` AS validator_index,
        lower(concat('0x', substring(withdrawal_credentials, 27))) AS cred_addr
    FROM {{ index .dep "{{external}}" "canonical_beacon_validators_withdrawal_credentials" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
),
pubkeys AS (
    SELECT `index` AS validator_index, pubkey
    FROM {{ index .dep "{{external}}" "canonical_beacon_validators_pubkeys" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.validator_index,
    ifNull(pk.pubkey, '') AS pubkey,
    m.node_operator,
    'minipool' AS pool_type,
    m.pool_address,
    m.created_date_time
FROM creds AS c
GLOBAL INNER JOIN minipools AS m ON c.cred_addr = m.pool_address
GLOBAL LEFT JOIN pubkeys AS pk ON c.validator_index = pk.validator_index

UNION ALL

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.validator_index,
    ifNull(pk.pubkey, '') AS pubkey,
    m.node_operator,
    'megapool' AS pool_type,
    m.pool_address,
    m.created_date_time
FROM creds AS c
GLOBAL INNER JOIN megapools AS m ON c.cred_addr = m.pool_address
GLOBAL LEFT JOIN pubkeys AS pk ON c.validator_index = pk.validator_index;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
