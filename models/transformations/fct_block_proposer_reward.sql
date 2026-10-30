---
table: fct_block_proposer_reward
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - reward
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_block_reward"
---
-- Consensus layer reward of each canonical block's proposer, from the Beacon API
-- block rewards Cannon stores (from Altair). Execution layer rewards are not
-- included: relay-reported MEV payments per block are in fct_block_mev.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    toString(block_root) AS block_root,
    proposer_index,
    total,
    attestations,
    sync_aggregate,
    proposer_slashings,
    attester_slashings
FROM {{ index .dep "{{external}}" "canonical_beacon_block_reward" "helpers" "from" }} FINAL
WHERE meta_network_name = '{{ .env.NETWORK }}'
  AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
