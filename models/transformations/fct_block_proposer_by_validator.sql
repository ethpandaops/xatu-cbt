---
table: fct_block_proposer_by_validator
type: incremental
interval:
  type: slot
  max: 100000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - proposer
  - canonical
  - validator_performance
dependencies:
  - "{{transformation}}.fct_block_proposer"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH deduped AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        proposer_validator_index,
        argMax(proposer_pubkey, updated_date_time) AS proposer_pubkey,
        argMax(block_root, updated_date_time) AS block_root,
        argMax(status, updated_date_time) AS status
    FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, proposer_validator_index
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    proposer_validator_index AS validator_index,
    proposer_pubkey AS pubkey,
    block_root,
    status
FROM deduped
