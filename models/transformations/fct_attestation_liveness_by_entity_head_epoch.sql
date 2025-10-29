---
table: fct_attestation_liveness_by_entity_head_epoch
type: incremental
interval:
  type: slot
  max: 50000
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    epoch,
    epoch_start_date_time,
    entity,
    status,
    SUM(attestation_count) as attestation_count
FROM {{ index .dep "{{transformation}}" "fct_attestation_liveness_by_entity_head" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY epoch, epoch_start_date_time, entity, status
