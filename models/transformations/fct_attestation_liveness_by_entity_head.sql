---
table: fct_attestation_liveness_by_entity_head
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - attestation
  - head
  - entity
dependencies:
  - "{{transformation}}.fct_attestation_correctness_by_validator_head"
  - "{{transformation}}.dim_node"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH attestations_with_entity AS (
    SELECT
        acv.slot,
        acv.slot_start_date_time,
        acv.epoch,
        acv.epoch_start_date_time,
        COALESCE(dn.source, 'unknown') AS entity,
        acv.block_root
    FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_by_validator_head" "helpers" "from" }} AS acv FINAL
    GLOBAL LEFT JOIN {{ index .dep "{{transformation}}" "dim_node" "helpers" "from" }} AS dn FINAL
        ON acv.attesting_validator_index = dn.validator_index
    WHERE acv.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    entity,
    countIf(block_root IS NOT NULL) as attestation_count,
    countIf(block_root IS NULL) as missed_count
FROM attestations_with_entity
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, entity
SETTINGS join_use_nulls = 1
