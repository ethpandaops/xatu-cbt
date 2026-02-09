---
table: fct_sync_committee_participation_by_validator
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - slot
  - sync_committee
  - canonical
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_block_sync_aggregate"
---
-- Per-slot sync committee participation by validator. One row per validator per slot
-- where the validator was part of the sync committee.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    validator_index,
    participated
FROM (
    -- Validators that participated
    SELECT
        slot,
        slot_start_date_time,
        arrayJoin(validators_participated) AS validator_index,
        true AS participated
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_sync_aggregate" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
      AND meta_network_name = '{{ .env.NETWORK }}'

    UNION ALL

    -- Validators that missed
    SELECT
        slot,
        slot_start_date_time,
        arrayJoin(validators_missed) AS validator_index,
        false AS participated
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_sync_aggregate" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
      AND meta_network_name = '{{ .env.NETWORK }}'
)
