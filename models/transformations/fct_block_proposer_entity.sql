---
table: fct_block_proposer_entity
interval:
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - mev
  - bid
dependencies:
  - "{{transformation}}.fct_block_proposer_head"
  # TODO: add when fixed dim_node as a static transformation
  # - "{{transformation}}.dim_node"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    bph.slot,
    bph.slot_start_date_time,
    bph.epoch,
    bph.epoch_start_date_time,
    dn.name as entity
FROM `{{ index .dep "{{transformation}}" "fct_block_proposer_head" "database" }}`.`fct_block_proposer_head` AS bph FINAL
GLOBAL LEFT JOIN `{{ index .dep "{{transformation}}" "fct_block_proposer_head" "database" }}`.`dim_node` AS dn FINAL
    ON bph.proposer_validator_index = dn.validator_index
    AND dn.source = 'entity'
WHERE bph.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
