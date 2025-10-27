---
table: fct_block_proposer_entity
type: incremental
interval:
  type: slot
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
  - "{{transformation}}.dim_node"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    bph.slot,
    bph.slot_start_date_time,
    bph.epoch,
    bph.epoch_start_date_time,
    dn.source as entity
FROM {{ index .dep "{{transformation}}" "fct_block_proposer_head" "helpers" "from" }} AS bph FINAL
GLOBAL LEFT JOIN {{ index .dep "{{transformation}}" "dim_node" "helpers" "from" }} AS dn FINAL
    ON bph.proposer_validator_index = dn.validator_index
WHERE bph.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
SETTINGS join_use_nulls = 1;
