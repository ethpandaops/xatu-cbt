---
table: fct_peer_count_by_custody_bucket_monthly
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

-- Get all data within month boundaries with custody bucketing
monthly_data AS (
    SELECT
        toDate(toStartOfMonth(epoch_start_date_time)) AS month_start_date,
        epoch,
        CASE
            WHEN custody_group_count BETWEEN 1 AND 4 THEN '1-4'
            WHEN custody_group_count BETWEEN 5 AND 8 THEN '5-8'
            WHEN custody_group_count BETWEEN 9 AND 16 THEN '9-16'
            WHEN custody_group_count BETWEEN 17 AND 32 THEN '17-32'
            WHEN custody_group_count BETWEEN 33 AND 64 THEN '33-64'
            WHEN custody_group_count BETWEEN 65 AND 127 THEN '65-127'
            WHEN custody_group_count = 128 THEN '128'
        END AS custody_bucket,
        peer_id_unique_key
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE toStartOfMonth(epoch_start_date_time) >= (SELECT min_month FROM month_bounds)
      AND toStartOfMonth(epoch_start_date_time) <= (SELECT max_month FROM month_bounds)
      AND custody_group_count >= 1
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    month_start_date,
    custody_bucket,
    toUInt32(count(DISTINCT epoch)) AS epoch_count,
    toUInt32(countDistinct(peer_id_unique_key)) AS peer_count
FROM monthly_data
GROUP BY
    month_start_date,
    custody_bucket
