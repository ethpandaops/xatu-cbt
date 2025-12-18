---
table: fct_peer_count_by_custody_bucket_top_asn_weekly
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
  - asn
dependencies:
  - "{{transformation}}.int_peer_custody_count_by_epoch"
---
-- Uses boundary expansion pattern: expand bounds to complete week boundaries (Monday start)
-- Top 20 ASNs by peer count are tracked individually, rest grouped as "Other"
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

-- Calculate top 20 ASNs by distinct peer count within the bounded data
top_asns AS (
    SELECT peer_geo_autonomous_system_organization
    FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} FINAL
    WHERE toStartOfWeek(epoch_start_date_time, 1) >= (SELECT min_week FROM week_bounds)
      AND toStartOfWeek(epoch_start_date_time, 1) <= (SELECT max_week FROM week_bounds)
      AND peer_geo_autonomous_system_organization != ''
      AND custody_group_count >= 1
    GROUP BY peer_geo_autonomous_system_organization
    ORDER BY countDistinct(peer_id_unique_key) DESC
    LIMIT 20
),

-- Get all data within week boundaries with custody bucketing and ASN grouping
weekly_data AS (
    SELECT
        toDate(toStartOfWeek(epoch_start_date_time, 1)) AS week_start_date,
        epoch,
        IF(
            peer_geo_autonomous_system_organization IN (SELECT peer_geo_autonomous_system_organization FROM top_asns),
            peer_geo_autonomous_system_organization,
            'Other'
        ) AS asn_organization,
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
      AND peer_geo_autonomous_system_organization != ''
      AND custody_group_count >= 1
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    week_start_date,
    asn_organization,
    custody_bucket,
    toUInt32(count(DISTINCT epoch)) AS epoch_count,
    toUInt32(countDistinct(peer_id_unique_key)) AS peer_count
FROM weekly_data
GROUP BY
    week_start_date,
    asn_organization,
    custody_bucket
