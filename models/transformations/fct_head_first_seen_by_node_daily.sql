---
table: fct_head_first_seen_by_node_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - head
  - node
dependencies:
  - "{{transformation}}.fct_head_first_seen_by_node"
---
-- When each node first saw head events, one row per node per day.
-- Rewrites every day the bounds touch so partial days are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toStartOfDay(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY AS max_day
),
-- One row per node per slot first, so a node reporting a slot twice (e.g. under a new client name) counts once
per_slot AS (
    SELECT
        node_id,
        slot_start_date_time,
        min(seen_slot_start_diff) AS seen_slot_start_diff,
        argMax(username, slot_start_date_time) AS username,
        argMax(classification, slot_start_date_time) AS classification,
        argMax(meta_client_name, slot_start_date_time) AS meta_client_name,
        argMax(meta_client_implementation, slot_start_date_time) AS meta_client_implementation,
        argMax(meta_client_version, slot_start_date_time) AS meta_client_version,
        argMax(meta_consensus_implementation, slot_start_date_time) AS meta_consensus_implementation,
        argMax(meta_consensus_version, slot_start_date_time) AS meta_consensus_version
    FROM {{ index .dep "{{transformation}}" "fct_head_first_seen_by_node" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
        AND slot_start_date_time < (SELECT max_day FROM day_bounds)
        AND node_id != ''
    GROUP BY node_id, slot_start_date_time
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
    node_id,
    argMax(username, slot_start_date_time) AS username,
    argMax(classification, slot_start_date_time) AS classification,
    argMax(meta_client_name, slot_start_date_time) AS meta_client_name,
    argMax(meta_client_implementation, slot_start_date_time) AS meta_client_implementation,
    argMax(meta_client_version, slot_start_date_time) AS meta_client_version,
    argMax(meta_consensus_implementation, slot_start_date_time) AS meta_consensus_implementation,
    argMax(meta_consensus_version, slot_start_date_time) AS meta_consensus_version,
    toUInt32(count()) AS slot_count,
    toUInt32(min(seen_slot_start_diff)) AS min_seen_slot_start_diff,
    toUInt32(round(quantiles(0.1, 0.5, 0.9, 0.95)(seen_slot_start_diff)[1])) AS p10_seen_slot_start_diff,
    toUInt32(round(quantiles(0.1, 0.5, 0.9, 0.95)(seen_slot_start_diff)[2])) AS p50_seen_slot_start_diff,
    toUInt32(round(quantiles(0.1, 0.5, 0.9, 0.95)(seen_slot_start_diff)[3])) AS p90_seen_slot_start_diff,
    toUInt32(round(quantiles(0.1, 0.5, 0.9, 0.95)(seen_slot_start_diff)[4])) AS p95_seen_slot_start_diff,
    toUInt32(max(seen_slot_start_diff)) AS max_seen_slot_start_diff
FROM per_slot
GROUP BY day_start_date, node_id
