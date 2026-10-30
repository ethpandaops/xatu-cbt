---
table: fct_sync_committee_reward_by_validator_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - sync_committee
  - reward
  - validator_performance
dependencies:
  - "{{transformation}}.fct_sync_committee_reward_by_validator_daily"
---
-- Sync committee rewards and penalties per validator per week (Monday to Sunday,
-- UTC), summed from fct_sync_committee_reward_by_validator_daily. Each run
-- rewrites every week it touches in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    week_bounds AS (
        SELECT
            toMonday(toDate(fromUnixTimestamp({{ .bounds.start }}))) AS min_week,
            toMonday(toDate(fromUnixTimestamp({{ .bounds.end }}))) AS max_week
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toMonday(day_start_date) AS week_start_date,
    validator_index,
    toUInt32(sum(slot_count)) AS slot_count,
    sum(total) AS total,
    sum(reward) AS reward,
    sum(penalty) AS penalty,
    toUInt32(sum(penalized_slot_count)) AS penalized_slot_count
FROM (
    SELECT day_start_date, validator_index, slot_count, total, reward, penalty, penalized_slot_count
    FROM {{ index .dep "{{transformation}}" "fct_sync_committee_reward_by_validator_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= (SELECT min_week FROM week_bounds)
      AND day_start_date < (SELECT max_week FROM week_bounds) + 7
)
GROUP BY week_start_date, validator_index
