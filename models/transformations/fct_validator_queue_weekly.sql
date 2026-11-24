---
table: fct_validator_queue_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - validator
  - queue
dependencies:
  - "{{transformation}}.fct_validator_queue_daily"
---
-- Activation and exit queues and the Electra pending queues for each week (Monday to Sunday, UTC): the snapshot of the week's last
-- day (Sunday, or the latest day so far for the current week) from fct_validator_queue_daily,
-- which only has days whose snapshot epoch was complete. A week whose last day is
-- missing there is not written. Each run also rewrites the week of the day before
-- the interval, so a week's last day is taken once it is final. Weeks before that
-- which are missing from this table are retried, up to 4 per run. The version is
-- the snapshot epoch.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    retry_weeks AS (
        SELECT week_start_date
        FROM (
            SELECT ifNull((SELECT min(week_start_date) FROM `{{ .self.database }}`.`{{ .self.table }}`), toDate(0)) + number * 7 AS week_start_date
            FROM numbers(toUInt64(greatest(0, intDiv(dateDiff('day',
                ifNull((SELECT min(week_start_date) FROM `{{ .self.database }}`.`{{ .self.table }}`), toDate(0)),
                toMonday(toDate(fromUnixTimestamp({{ .bounds.start }})))), 7))))
        )
        WHERE (SELECT count() FROM `{{ .self.database }}`.`{{ .self.table }}`) > 0
          AND week_start_date GLOBAL NOT IN (SELECT DISTINCT week_start_date FROM `{{ .self.database }}`.`{{ .self.table }}`)
        ORDER BY week_start_date
        LIMIT 4
    ),
    target_days AS (
        SELECT
            toMonday(toDate(fromUnixTimestamp({{ .bounds.start }})) - 1) + number * 7 AS week_start_date,
            least(week_start_date + 6, toDate(fromUnixTimestamp({{ .bounds.end }}))) AS day_start_date
        FROM numbers(toUInt64(intDiv(dateDiff('day', toMonday(toDate(fromUnixTimestamp({{ .bounds.start }})) - 1), toMonday(toDate(fromUnixTimestamp({{ .bounds.end }})))), 7) + 1))
        UNION ALL
        SELECT week_start_date, week_start_date + 6 AS day_start_date
        FROM retry_weeks
    ),
    snapshots AS (
        SELECT
            t.week_start_date AS week_start_date,
            d.*
        FROM {{ index .dep "{{transformation}}" "fct_validator_queue_daily" "helpers" "from" }} AS d FINAL
        GLOBAL INNER JOIN target_days AS t ON t.day_start_date = d.day_start_date
        WHERE d.day_start_date GLOBAL IN (SELECT day_start_date FROM target_days)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    week_start_date,
    epoch,
    epoch_start_date_time,
    pending_initialized_count,
    pending_initialized_balance,
    activation_queue_count,
    activation_queue_effective_balance,
    exit_queue_count,
    exit_queue_effective_balance,
    pending_deposit_count,
    pending_deposit_amount,
    pending_consolidation_count,
    pending_consolidation_source_effective_balance,
    pending_partial_withdrawal_count,
    pending_partial_withdrawal_amount
FROM snapshots
