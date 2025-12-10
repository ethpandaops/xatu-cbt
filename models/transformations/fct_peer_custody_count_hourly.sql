---
table: fct_peer_custody_count_hourly
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - hourly
  - peerdas
  - custody
  - peer
dependencies:
  - "{{transformation}}.int_peer_custody_count_by_epoch"
---
-- Uses boundary expansion pattern: expand bounds to complete hour boundaries
-- ReplacingMergeTree handles re-aggregation of partial hours at the head
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Find hour boundaries for current epoch range
hour_bounds AS (
    SELECT
        toStartOfHour(min(epoch_start_date_time)) AS min_hour,
        toStartOfHour(max(epoch_start_date_time)) AS max_hour
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

-- Get all data within hour boundaries
hourly_data AS (
    SELECT
        toStartOfHour(epoch_start_date_time) AS hour_start_date_time,
        epoch,
        custody_group_count,
        peer_id_unique_key
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE toStartOfHour(epoch_start_date_time) >= (SELECT min_hour FROM hour_bounds)
      AND toStartOfHour(epoch_start_date_time) <= (SELECT max_hour FROM hour_bounds)
),

-- Calculate epoch-level peer counts for min/max/avg statistics
epoch_counts AS (
    SELECT
        hour_start_date_time,
        epoch,
        custody_group_count,
        toUInt32(countDistinct(peer_id_unique_key)) AS epoch_peer_count
    FROM hourly_data
    GROUP BY hour_start_date_time, epoch, custody_group_count
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    custody_group_count,
    toUInt32(count(DISTINCT epoch)) AS epoch_count,
    toUInt32(countDistinct(peer_id_unique_key)) AS peer_count,
    min(ec.epoch_peer_count) AS min_epoch_peer_count,
    max(ec.epoch_peer_count) AS max_epoch_peer_count,
    avg(ec.epoch_peer_count) AS avg_epoch_peer_count
FROM hourly_data hd
INNER JOIN epoch_counts ec ON hd.hour_start_date_time = ec.hour_start_date_time
    AND hd.epoch = ec.epoch
    AND hd.custody_group_count = ec.custody_group_count
GROUP BY
    hour_start_date_time,
    custody_group_count
