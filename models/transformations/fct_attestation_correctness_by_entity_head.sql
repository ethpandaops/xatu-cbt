---
table: fct_attestation_correctness_by_entity_head
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
        dn.source AS entity,
        acv.block_root
    FROM `{{ index .dep "{{transformation}}" "fct_attestation_correctness_by_validator_head" "database" }}`.`fct_attestation_correctness_by_validator_head` AS acv FINAL
    GLOBAL LEFT JOIN `{{ .self.database }}`.`dim_node` AS dn FINAL
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
    block_root,
    COUNT(*) as attestation_count
FROM attestations_with_entity
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, entity, block_root
SETTINGS join_use_nulls = 1
