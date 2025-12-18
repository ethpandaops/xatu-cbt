---
table: fct_peer_count_by_custody_bucket_client_weekly
type: incremental
interval:
  type: slot
  max: 100000
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 1m"
tags:
  - weekly
  - peerdas
  - custody
  - peer
  - client
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

-- Get all data within week boundaries with custody bucketing
weekly_data AS (
    SELECT
        toDate(toStartOfWeek(epoch_start_date_time, 1)) AS week_start_date,
        epoch,
        peer_agent_implementation,
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
    WHERE toStartOfWeek(epoch_start_date_time, 1) >= (SELECT min_week FROM week_bounds)
      AND toStartOfWeek(epoch_start_date_time, 1) <= (SELECT max_week FROM week_bounds)
      AND peer_agent_implementation != ''
      AND custody_group_count >= 1
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    week_start_date,
    peer_agent_implementation,
    custody_bucket,
    toUInt32(count(DISTINCT epoch)) AS epoch_count,
    toUInt32(countDistinct(peer_id_unique_key)) AS peer_count
FROM weekly_data
GROUP BY
    week_start_date,
    peer_agent_implementation,
    custody_bucket
