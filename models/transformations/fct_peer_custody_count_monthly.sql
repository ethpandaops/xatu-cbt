---
table: fct_peer_custody_count_monthly
type: incremental
interval:
  type: slot
  max: 500000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 1m"
tags:
  - monthly
  - peerdas
  - custody
  - peer
dependencies:
  - "{{transformation}}.int_peer_custody_count_by_epoch"
---
-- Uses boundary expansion pattern: expand bounds to complete month boundaries
-- ReplacingMergeTree handles re-aggregation of partial months at the head
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Find month boundaries for current epoch range
month_bounds AS (
    SELECT
        toStartOfMonth(min(epoch_start_date_time)) AS min_month,
        toStartOfMonth(max(epoch_start_date_time)) AS max_month
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

-- Get all data within month boundaries
monthly_data AS (
    SELECT
        toDate(toStartOfMonth(epoch_start_date_time)) AS month_start_date,
        epoch,
        custody_group_count,
        peer_id_unique_key
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE toStartOfMonth(epoch_start_date_time) >= (SELECT min_month FROM month_bounds)
      AND toStartOfMonth(epoch_start_date_time) <= (SELECT max_month FROM month_bounds)
),

-- Calculate epoch-level peer counts for min/max/avg statistics
epoch_counts AS (
    SELECT
        month_start_date,
        epoch,
        custody_group_count,
        toUInt32(countDistinct(peer_id_unique_key)) AS epoch_peer_count
    FROM monthly_data
    GROUP BY month_start_date, epoch, custody_group_count
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    month_start_date,
    custody_group_count,
    toUInt32(count(DISTINCT epoch)) AS epoch_count,
    toUInt32(countDistinct(peer_id_unique_key)) AS peer_count,
    min(ec.epoch_peer_count) AS min_epoch_peer_count,
    max(ec.epoch_peer_count) AS max_epoch_peer_count,
    avg(ec.epoch_peer_count) AS avg_epoch_peer_count
FROM monthly_data md
INNER JOIN epoch_counts ec ON md.month_start_date = ec.month_start_date
    AND md.epoch = ec.epoch
    AND md.custody_group_count = ec.custody_group_count
GROUP BY
    month_start_date,
    custody_group_count
