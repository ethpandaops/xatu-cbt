---
table: fct_sync_committee_reward_by_validator
type: incremental
interval:
  type: slot
  max: 21600
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - slot
  - sync_committee
  - reward
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_sync_committee_reward"
  - "{{transformation}}.fct_sync_committee_participation_by_validator"
---
-- Sync committee reward of each validator in each block, from the Beacon API sync
-- committee rewards Cannon stores (negative for a missed vote), keyed by validator
-- so one validator's slots are a primary key range. It covers the same range as
-- the per-duty rows in fct_sync_committee_participation_by_validator (a
-- dependency for its bounds).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    validator_index,
    reward
FROM {{ index .dep "{{external}}" "canonical_beacon_sync_committee_reward" "helpers" "from" }} FINAL
WHERE meta_network_name = '{{ .env.NETWORK }}'
  AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
