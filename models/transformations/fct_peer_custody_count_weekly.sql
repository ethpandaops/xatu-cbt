---
table: fct_peer_custody_count_weekly
type: incremental
interval:
  type: slot
  max: 200000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 1m"
tags:
  - weekly
  - peerdas
  - custody
  - peer
dependencies:
  - "{{transformation}}.int_peer_custody_count_by_epoch"
---
-- Uses boundary expansion pattern: expand bounds to complete week boundaries (Monday start)
-- ReplacingMergeTree handles re-aggregation of partial weeks at the head
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Find week boundaries for current epoch range (Monday start)
week_bounds AS (
    SELECT
        toStartOfWeek(min(epoch_start_date_time), 1) AS min_week,
        toStartOfWeek(max(epoch_start_date_time), 1) AS max_week
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

-- Get all data within week boundaries
weekly_data AS (
    SELECT
        toDate(toStartOfWeek(epoch_start_date_time, 1)) AS week_start_date,
        epoch,
        custody_group_count,
        peer_id_unique_key
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE toStartOfWeek(epoch_start_date_time, 1) >= (SELECT min_week FROM week_bounds)
      AND toStartOfWeek(epoch_start_date_time, 1) <= (SELECT max_week FROM week_bounds)
),

-- Calculate epoch-level peer counts for min/max/avg statistics
epoch_counts AS (
    SELECT
        week_start_date,
        epoch,
        custody_group_count,
        toUInt32(countDistinct(peer_id_unique_key)) AS epoch_peer_count
    FROM weekly_data
    GROUP BY week_start_date, epoch, custody_group_count
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    week_start_date,
    custody_group_count,
    toUInt32(count(DISTINCT epoch)) AS epoch_count,
    toUInt32(countDistinct(peer_id_unique_key)) AS peer_count,
    min(ec.epoch_peer_count) AS min_epoch_peer_count,
    max(ec.epoch_peer_count) AS max_epoch_peer_count,
    avg(ec.epoch_peer_count) AS avg_epoch_peer_count
FROM weekly_data wd
INNER JOIN epoch_counts ec ON wd.week_start_date = ec.week_start_date
    AND wd.epoch = ec.epoch
    AND wd.custody_group_count = ec.custody_group_count
GROUP BY
    week_start_date,
    custody_group_count
