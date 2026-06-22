---
table: dim_rocketpool_node
type: scheduled
schedule: "@every 1h"
tags:
  - node
  - rocketpool
dependencies:
  - "{{transformation}}.int_rocketpool_minipool"
  - "{{transformation}}.fct_rocketpool_validator"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH validators AS (
    SELECT
        node_operator,
        count() AS validator_count
    FROM {{ index .dep "{{transformation}}" "fct_rocketpool_validator" "helpers" "from" }} FINAL
    GROUP BY node_operator
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    m.node_operator,
    m.minipool_count,
    m.active_minipool_count,
    ifNull(v.validator_count, 0) AS validator_count,
    m.first_minipool_date_time,
    m.last_minipool_date_time
FROM
(
    SELECT
        node_operator,
        countIf(event_name = 'created') AS minipool_count,
        toUInt32(countIf(event_name = 'created') - countIf(event_name = 'destroyed')) AS active_minipool_count,
        min(event_date_time) AS first_minipool_date_time,
        max(event_date_time) AS last_minipool_date_time
    FROM {{ index .dep "{{transformation}}" "int_rocketpool_minipool" "helpers" "from" }} FINAL
    WHERE event_name IN ('created', 'destroyed')
    GROUP BY node_operator
) AS m
GLOBAL LEFT JOIN validators AS v ON m.node_operator = v.node_operator;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
