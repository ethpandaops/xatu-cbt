---
table: fct_peer_custody_count_daily
type: incremental
interval:
  type: slot
  max: 100000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 1m"
tags:
  - daily
  - peerdas
  - custody
  - peer
dependencies:
  - "{{transformation}}.int_peer_custody_count_by_epoch"
---
-- Uses boundary expansion pattern: expand bounds to complete day boundaries
-- ReplacingMergeTree handles re-aggregation of partial days at the head
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Find day boundaries for current epoch range
day_bounds AS (
    SELECT
        toDate(min(epoch_start_date_time)) AS min_day,
        toDate(max(epoch_start_date_time)) AS max_day
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

-- Get all data within day boundaries
daily_data AS (
    SELECT
        toDate(epoch_start_date_time) AS day_start_date,
        epoch,
        custody_group_count,
        peer_id_unique_key
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE toDate(epoch_start_date_time) >= (SELECT min_day FROM day_bounds)
      AND toDate(epoch_start_date_time) <= (SELECT max_day FROM day_bounds)
),

-- Calculate epoch-level peer counts for min/max/avg statistics
epoch_counts AS (
    SELECT
        day_start_date,
        epoch,
        custody_group_count,
        toUInt32(countDistinct(peer_id_unique_key)) AS epoch_peer_count
    FROM daily_data
    GROUP BY day_start_date, epoch, custody_group_count
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    custody_group_count,
    toUInt32(count(DISTINCT epoch)) AS epoch_count,
    toUInt32(countDistinct(peer_id_unique_key)) AS peer_count,
    min(ec.epoch_peer_count) AS min_epoch_peer_count,
    max(ec.epoch_peer_count) AS max_epoch_peer_count,
    avg(ec.epoch_peer_count) AS avg_epoch_peer_count
FROM daily_data dd
INNER JOIN epoch_counts ec ON dd.day_start_date = ec.day_start_date
    AND dd.epoch = ec.epoch
    AND dd.custody_group_count = ec.custody_group_count
GROUP BY
    day_start_date,
    custody_group_count
