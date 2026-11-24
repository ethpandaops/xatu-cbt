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
-- Consensus layer reward of each canonical block's proposer, from the block
-- rewards Cannon stores: the Beacon API rewards from Altair, and rewards computed
-- from beacon states before Altair (phase0, which has no sync aggregate). The
-- external's bounds only reach into phase0 once both tables join up. Execution layer rewards are not
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
FROM (
    SELECT slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, proposer_index,
           total, attestations, sync_aggregate, proposer_slashings, attester_slashings
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_reward" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
      AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    UNION ALL
    SELECT slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, proposer_index,
           total, attestations, toUInt64(0) AS sync_aggregate, proposer_slashings, attester_slashings
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_reward" "helpers" "from" | replace "`canonical_beacon_block_reward" "`canonical_beacon_phase0_block_reward" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
      AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
