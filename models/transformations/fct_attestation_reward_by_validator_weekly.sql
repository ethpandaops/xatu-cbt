---
table: fct_attestation_reward_by_validator_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - attestation
  - reward
  - validator_performance
dependencies:
  - "{{transformation}}.fct_attestation_reward_by_validator_daily"
---
-- Attestation rewards and penalties per validator per week (Monday to Sunday,
-- UTC), summed from fct_attestation_reward_by_validator_daily. Each run rewrites
-- every week it touches in full.
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
    toUInt32(sum(epoch_count)) AS epoch_count,
    sum(head) AS head,
    sum(target) AS target,
    sum(source) AS source,
    sum(inclusion_delay) AS inclusion_delay,
    sum(inactivity) AS inactivity,
    sum(total) AS total,
    sum(penalty) AS penalty,
    toUInt32(sum(penalized_epoch_count)) AS penalized_epoch_count
FROM (
    SELECT
        day_start_date, validator_index,
        epoch_count, head, target, source, inclusion_delay, inactivity, total, penalty, penalized_epoch_count
    FROM {{ index .dep "{{transformation}}" "fct_attestation_reward_by_validator_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= (SELECT min_week FROM week_bounds)
      AND day_start_date < (SELECT max_week FROM week_bounds) + 7
)
GROUP BY week_start_date, validator_index
SETTINGS max_bytes_before_external_group_by = 10000000000
