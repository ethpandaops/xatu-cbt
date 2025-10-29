---
table: fct_attestation_liveness_by_entity_head_daily
type: incremental
interval:
  type: slot
  max: 50000
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    toDate(epoch_start_date_time) as date,
    entity,
    status,
    SUM(attestation_count) as attestation_count
FROM {{ index .dep "{{transformation}}" "fct_attestation_liveness_by_entity_head_epoch" "helpers" "from" }} FINAL
WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY toDate(epoch_start_date_time), entity, status
