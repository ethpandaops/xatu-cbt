---
table: fct_validator_reward_total_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - reward
  - validator_performance
dependencies:
  - "{{transformation}}.fct_validator_reward_total_daily"
  - "{{transformation}}.fct_attestation_reward_by_validator_daily"
---
-- Network-wide consensus layer rewards and penalties per week (Monday to Sunday,
-- UTC), summed from fct_validator_reward_total_daily. attesting_validator_count
-- is the number of distinct validators with an attestation reward in the week.
-- Each run rewrites every week it touches in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    week_bounds AS (
        SELECT
            toMonday(toDate(fromUnixTimestamp({{ .bounds.start }}))) AS min_week,
            toMonday(toDate(fromUnixTimestamp({{ .bounds.end }}))) AS max_week
    ),
    totals AS (
        SELECT
            toMonday(day_start_date) AS week_start_date,
            sum(attestation_head) AS attestation_head,
            sum(attestation_target) AS attestation_target,
            sum(attestation_source) AS attestation_source,
            sum(attestation_inactivity) AS attestation_inactivity,
            sum(attestation_total) AS attestation_total,
            sum(attestation_penalty) AS attestation_penalty,
            sum(sync_committee_reward) AS sync_committee_reward,
            sum(sync_committee_penalty) AS sync_committee_penalty,
            sum(block_count) AS block_count,
            sum(proposer_total) AS proposer_total,
            sum(proposer_attestations) AS proposer_attestations,
            sum(proposer_sync_aggregate) AS proposer_sync_aggregate,
            sum(proposer_slashings) AS proposer_slashings
        FROM {{ index .dep "{{transformation}}" "fct_validator_reward_total_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_week FROM week_bounds)
          AND day_start_date < (SELECT max_week FROM week_bounds) + 7
        GROUP BY week_start_date
    ),
    attesters AS (
        SELECT toMonday(day_start_date) AS week_start_date, uniqExact(validator_index) AS attesting_validator_count
        FROM {{ index .dep "{{transformation}}" "fct_attestation_reward_by_validator_daily" "helpers" "from" }}
        WHERE day_start_date >= (SELECT min_week FROM week_bounds)
          AND day_start_date < (SELECT max_week FROM week_bounds) + 7
        GROUP BY week_start_date
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    t.week_start_date AS week_start_date,
    toUInt32(ifNull(a.attesting_validator_count, 0)) AS attesting_validator_count,
    t.attestation_head AS attestation_head,
    t.attestation_target AS attestation_target,
    t.attestation_source AS attestation_source,
    t.attestation_inactivity AS attestation_inactivity,
    t.attestation_total AS attestation_total,
    t.attestation_penalty AS attestation_penalty,
    t.sync_committee_reward AS sync_committee_reward,
    t.sync_committee_penalty AS sync_committee_penalty,
    toUInt32(t.block_count) AS block_count,
    toUInt64(t.proposer_total) AS proposer_total,
    toUInt64(t.proposer_attestations) AS proposer_attestations,
    toUInt64(t.proposer_sync_aggregate) AS proposer_sync_aggregate,
    toUInt64(t.proposer_slashings) AS proposer_slashings
FROM totals AS t
GLOBAL LEFT JOIN attesters AS a ON a.week_start_date = t.week_start_date
SETTINGS join_use_nulls = 1
