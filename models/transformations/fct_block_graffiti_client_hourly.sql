---
table: fct_block_graffiti_client_hourly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - block
  - graffiti
  - client
dependencies:
  - "{{transformation}}.fct_block_graffiti"
---
-- Rewrites every hour the bounds touch so partial hours are recounted in full. Keys stored for those hours
-- that the recount no longer produces (a reprocess moved their blocks to another key) are rewritten as zero.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    consensus_client,
    execution_client,
    toUInt32(sum(blocks)) AS block_count
FROM (
    SELECT
        toStartOfHour(slot_start_date_time) AS hour_start_date_time,
        consensus_client,
        execution_client,
        count() AS blocks
    FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
      AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    GROUP BY hour_start_date_time, consensus_client, execution_client

    UNION ALL

    SELECT
        hour_start_date_time,
        consensus_client,
        execution_client,
        toUInt64(0) AS blocks
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE hour_start_date_time >= (SELECT min_hour FROM hour_bounds)
      AND hour_start_date_time <= (SELECT max_hour FROM hour_bounds)
      AND block_count > 0
)
GROUP BY hour_start_date_time, consensus_client, execution_client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
