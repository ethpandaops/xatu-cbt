---
table: dim_rocketpool_node
type: scheduled
schedule: "@every 1h"
tags:
  - node
  - rocketpool
dependencies:
  - "{{transformation}}.int_rocketpool_minipool"
  - "{{transformation}}.int_rocketpool_megapool_validator"
  - "{{transformation}}.int_rocketpool_node_event"
  - "{{transformation}}.int_rocketpool_node_timezone"
  - "{{transformation}}.fct_rocketpool_validator"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
mp AS (
    SELECT
        node_operator,
        countIf(event_name = 'created') AS minipool_count,
        toUInt32(countIf(event_name = 'created') - countIf(event_name = 'destroyed')) AS active_minipool_count,
        min(event_date_time) AS first_mp,
        max(event_date_time) AS last_mp
    FROM {{ index .dep "{{transformation}}" "int_rocketpool_minipool" "helpers" "from" }} FINAL
    WHERE event_name IN ('created', 'destroyed')
    GROUP BY node_operator
),
mega AS (
    SELECT
        node_operator,
        count() AS megapool_validator_count,
        min(deposit_date_time) AS first_mega,
        max(deposit_date_time) AS last_mega
    FROM {{ index .dep "{{transformation}}" "int_rocketpool_megapool_validator" "helpers" "from" }} FINAL
    GROUP BY node_operator
),
vc AS (
    SELECT node_operator, count() AS validator_count
    FROM {{ index .dep "{{transformation}}" "fct_rocketpool_validator" "helpers" "from" }} FINAL
    GROUP BY node_operator
),
reg AS (
    SELECT
        node_address,
        minIf(event_date_time, event_name = 'node_registered') AS registered_date_time,
        greatest(toInt256(0), sumIf(toInt256(rpl_amount_wei), event_name = 'rpl_staked') - sumIf(toInt256(rpl_amount_wei), event_name = 'rpl_withdrawn')) AS rpl_net,
        argMaxIf(smoothing_state, event_block_number, event_name = 'smoothing_pool_state_changed') AS in_smoothing_pool
    FROM {{ index .dep "{{transformation}}" "int_rocketpool_node_event" "helpers" "from" }} FINAL
    GROUP BY node_address
),
tz AS (
    SELECT node_address, argMax(timezone, set_block_number) AS timezone
    FROM {{ index .dep "{{transformation}}" "int_rocketpool_node_timezone" "helpers" "from" }} FINAL
    GROUP BY node_address
),
ops AS (
    SELECT node_operator FROM mp
    UNION DISTINCT
    SELECT node_operator FROM mega
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    o.node_operator,
    ifNull(tz.timezone, '') AS timezone,
    ifNull(nullIf(reg.registered_date_time, fromUnixTimestamp(0)), least(ifNull(mp.first_mp, toDateTime('2106-02-07 00:00:00')), ifNull(mega.first_mega, toDateTime('2106-02-07 00:00:00')))) AS registered_date_time,
    ifNull(mp.minipool_count, 0) AS minipool_count,
    ifNull(mp.active_minipool_count, 0) AS active_minipool_count,
    ifNull(mega.megapool_validator_count, 0) AS megapool_validator_count,
    ifNull(vc.validator_count, 0) AS validator_count,
    toUInt256(ifNull(reg.rpl_net, toInt256(0))) AS rpl_staked_wei,
    ifNull(reg.in_smoothing_pool, false) AS in_smoothing_pool,
    least(ifNull(mp.first_mp, toDateTime('2106-02-07 00:00:00')), ifNull(mega.first_mega, toDateTime('2106-02-07 00:00:00'))) AS first_seen_date_time,
    greatest(ifNull(mp.last_mp, fromUnixTimestamp(0)), ifNull(mega.last_mega, fromUnixTimestamp(0))) AS last_activity_date_time
FROM ops AS o
GLOBAL LEFT JOIN mp ON o.node_operator = mp.node_operator
GLOBAL LEFT JOIN mega ON o.node_operator = mega.node_operator
GLOBAL LEFT JOIN vc ON o.node_operator = vc.node_operator
GLOBAL LEFT JOIN reg ON o.node_operator = reg.node_address
GLOBAL LEFT JOIN tz ON o.node_operator = tz.node_address
SETTINGS join_use_nulls = 1;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
