---
table: fct_validator_reward_total_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - reward
  - validator_performance
dependencies:
  - "{{transformation}}.fct_attestation_reward_by_validator_daily"
  - "{{transformation}}.fct_sync_committee_reward_by_validator_daily"
  - "{{transformation}}.fct_block_proposer_reward"
---
-- Network-wide consensus layer rewards and penalties per day: attestation, sync
-- committee and block proposer rewards, summed over all validators. Each run
-- rewrites every day it touches in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    days AS (
        SELECT toDate(fromUnixTimestamp({{ .bounds.start }})) + number AS day_start_date
        FROM numbers(toUInt64(dateDiff('day', toDate(fromUnixTimestamp({{ .bounds.start }})), toDate(fromUnixTimestamp({{ .bounds.end }}))) + 1))
    ),
    attestation AS (
        SELECT
            day_start_date,
            count() AS attesting_validator_count,
            sum(head) AS head,
            sum(target) AS target,
            sum(source) AS source,
            sum(inactivity) AS inactivity,
            sum(total) AS total,
            sum(penalty) AS penalty
        FROM {{ index .dep "{{transformation}}" "fct_attestation_reward_by_validator_daily" "helpers" "from" }} FINAL
        WHERE day_start_date BETWEEN (SELECT min_day FROM day_bounds) AND (SELECT max_day FROM day_bounds)
        GROUP BY day_start_date
    ),
    sync_committee AS (
        SELECT
            day_start_date,
            sum(reward) AS reward,
            sum(penalty) AS penalty
        FROM {{ index .dep "{{transformation}}" "fct_sync_committee_reward_by_validator_daily" "helpers" "from" }} FINAL
        WHERE day_start_date BETWEEN (SELECT min_day FROM day_bounds) AND (SELECT max_day FROM day_bounds)
        GROUP BY day_start_date
    ),
    proposer AS (
        SELECT
            toDate(slot_start_date_time) AS day_start_date,
            count() AS block_count,
            sum(total) AS total,
            sum(attestations) AS attestations,
            sum(sync_aggregate) AS sync_aggregate,
            sum(proposer_slashings) + sum(attester_slashings) AS slashings
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer_reward" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= toDateTime((SELECT min_day FROM day_bounds))
          AND slot_start_date_time < toDateTime((SELECT max_day FROM day_bounds)) + INTERVAL 1 DAY
        GROUP BY day_start_date
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    d.day_start_date AS day_start_date,
    toUInt32(ifNull(a.attesting_validator_count, 0)) AS attesting_validator_count,
    ifNull(a.head, 0) AS attestation_head,
    ifNull(a.target, 0) AS attestation_target,
    ifNull(a.source, 0) AS attestation_source,
    ifNull(a.inactivity, 0) AS attestation_inactivity,
    ifNull(a.total, 0) AS attestation_total,
    ifNull(a.penalty, 0) AS attestation_penalty,
    ifNull(s.reward, 0) AS sync_committee_reward,
    ifNull(s.penalty, 0) AS sync_committee_penalty,
    toUInt32(ifNull(p.block_count, 0)) AS block_count,
    toUInt64(ifNull(p.total, 0)) AS proposer_total,
    toUInt64(ifNull(p.attestations, 0)) AS proposer_attestations,
    toUInt64(ifNull(p.sync_aggregate, 0)) AS proposer_sync_aggregate,
    toUInt64(ifNull(p.slashings, 0)) AS proposer_slashings
FROM days AS d
GLOBAL LEFT JOIN attestation AS a ON a.day_start_date = d.day_start_date
GLOBAL LEFT JOIN sync_committee AS s ON s.day_start_date = d.day_start_date
GLOBAL LEFT JOIN proposer AS p ON p.day_start_date = d.day_start_date
SETTINGS join_use_nulls = 1
