---
table: fct_validator_count_by_credential_type_weekly
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
  - credential
dependencies:
  - "{{transformation}}.fct_validator_count_by_credential_type_daily"
---
-- Validators and stake by withdrawal credential type for each week (Monday to Sunday, UTC): the snapshot of the week's last
-- day (Sunday, or the latest day so far for the current week) from fct_validator_count_by_credential_type_daily,
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
        FROM {{ index .dep "{{transformation}}" "fct_validator_count_by_credential_type_daily" "helpers" "from" }} AS d FINAL
        GLOBAL INNER JOIN target_days AS t ON t.day_start_date = d.day_start_date
        WHERE d.day_start_date GLOBAL IN (SELECT day_start_date FROM target_days)
    ),
    week_epochs AS (
        SELECT week_start_date, max(epoch) AS epoch, max(epoch_start_date_time) AS epoch_start_date_time
        FROM snapshots
        GROUP BY week_start_date
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    week_start_date,
    max(epoch) AS epoch,
    max(epoch_start_date_time) AS epoch_start_date_time,
    credential_type,
    toUInt32(sum(validator_count)) AS validator_count,
    toUInt32(sum(active_validator_count)) AS active_validator_count,
    toUInt64(sum(active_effective_balance)) AS active_effective_balance,
    toUInt64(sum(active_balance)) AS active_balance,
    toUInt32(sum(pending_validator_count)) AS pending_validator_count,
    toUInt32(sum(exiting_validator_count)) AS exiting_validator_count,
    toUInt32(sum(exited_validator_count)) AS exited_validator_count
FROM (
    SELECT
        week_start_date, epoch, epoch_start_date_time, credential_type,
        validator_count, active_validator_count, active_effective_balance, active_balance,
        pending_validator_count, exiting_validator_count, exited_validator_count
    FROM snapshots

    UNION ALL

    -- Credential types stored for these weeks that the snapshot no longer has.
    SELECT
        o.week_start_date, w.epoch, w.epoch_start_date_time, o.credential_type,
        0, 0, 0, 0, 0, 0, 0
    FROM `{{ .self.database }}`.`{{ .self.table }}` AS o FINAL
    GLOBAL INNER JOIN week_epochs AS w ON w.week_start_date = o.week_start_date
)
GROUP BY week_start_date, credential_type
SETTINGS parallel_distributed_insert_select = 0
