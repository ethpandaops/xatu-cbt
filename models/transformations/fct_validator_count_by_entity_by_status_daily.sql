---
table: fct_validator_count_by_entity_by_status_daily
type: incremental
interval:
  type: slot
  max: 259200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - day
  - entity
  - validator
  - status
dependencies:
  - "{{external}}.canonical_beacon_validators"
  - "{{transformation}}.dim_node"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_epochs AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) + number AS day,
            toDateTime(
                intDiv(
                    toInt64(toUnixTimestamp(toDateTime(toDate(fromUnixTimestamp({{ .bounds.start }})) + number))) - {{ .env.GENESIS_TIMESTAMP }},
                    384
                ) * 384 + {{ .env.GENESIS_TIMESTAMP }}
            ) AS epoch_start_dt
        FROM numbers(toUInt32(dateDiff('day', toDate(fromUnixTimestamp({{ .bounds.start }})), toDate(fromUnixTimestamp({{ .bounds.end }}))) + 1))
    ),
    validators_at_epochs AS (
        SELECT
            epoch_start_date_time,
            `index` AS validator_index,
            status
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time IN (SELECT epoch_start_dt FROM day_epochs)
    ),
    validators_with_day AS (
        SELECT
            de.day,
            va.validator_index,
            va.status
        FROM validators_at_epochs AS va
        INNER JOIN day_epochs AS de ON va.epoch_start_date_time = de.epoch_start_dt
    ),
    validators_with_entity AS (
        SELECT
            vd.day,
            if(dn.source = '', 'unknown', dn.source) AS entity,
            vd.status
        FROM validators_with_day AS vd
        GLOBAL LEFT JOIN {{ index .dep "{{transformation}}" "dim_node" "helpers" "from" }} AS dn FINAL
            ON vd.validator_index = dn.validator_index
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day AS day_start_date,
    entity,
    status,
    toUInt32(count()) AS validator_count
FROM validators_with_entity
GROUP BY day, entity, status
