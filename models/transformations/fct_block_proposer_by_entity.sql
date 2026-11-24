---
table: fct_block_proposer_by_entity
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - proposer
  - canonical
  - entity
dependencies:
  - "{{transformation}}.fct_block_proposer"
  - "{{transformation}}.dim_node"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    bp.slot,
    bp.slot_start_date_time,
    bp.epoch,
    bp.epoch_start_date_time,
    if(dn.source IS NULL OR dn.source = '', 'unknown', dn.source) AS entity,
    bp.proposer_validator_index,
    bp.block_root,
    bp.`status`
FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} AS bp FINAL
GLOBAL LEFT JOIN {{ index .dep "{{transformation}}" "dim_node" "helpers" "from" }} AS dn FINAL
    ON bp.proposer_validator_index = dn.validator_index
WHERE bp.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
SETTINGS join_use_nulls = 1
