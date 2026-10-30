---
table: fct_block_data_column_sidecar_first_seen_by_node_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - data_column
  - peerdas
  - node
dependencies:
  - "{{transformation}}.fct_block_data_column_sidecar_first_seen_by_node"
---
-- When each node saw its custody data columns, per slot from the first to the last column, one row per node per hour.
-- Rewrites every hour the bounds touch so partial hours are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
hour_bounds AS (
    SELECT
        toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_hour,
        toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 HOUR AS max_hour
),
-- One row per node per slot first, so a node reporting a slot twice (e.g. under a new client name) counts once
per_slot AS (
    SELECT
        node_id,
        slot_start_date_time,
        min(seen_slot_start_diff) AS first_seen_slot_start_diff,
        max(seen_slot_start_diff) AS last_seen_slot_start_diff,
        groupUniqArray(column_index) AS column_indexes,
        argMax(username, slot_start_date_time) AS username,
        argMax(classification, slot_start_date_time) AS classification,
        argMax(meta_client_name, slot_start_date_time) AS meta_client_name,
        argMax(meta_client_implementation, slot_start_date_time) AS meta_client_implementation,
        argMax(meta_client_version, slot_start_date_time) AS meta_client_version,
        argMax(meta_consensus_implementation, slot_start_date_time) AS meta_consensus_implementation,
        argMax(meta_consensus_version, slot_start_date_time) AS meta_consensus_version
    FROM {{ index .dep "{{transformation}}" "fct_block_data_column_sidecar_first_seen_by_node" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
        AND slot_start_date_time < (SELECT max_hour FROM hour_bounds)
        AND node_id != ''
    GROUP BY node_id, slot_start_date_time
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    node_id,
    argMax(username, slot_start_date_time) AS username,
    argMax(classification, slot_start_date_time) AS classification,
    argMax(meta_client_name, slot_start_date_time) AS meta_client_name,
    argMax(meta_client_implementation, slot_start_date_time) AS meta_client_implementation,
    argMax(meta_client_version, slot_start_date_time) AS meta_client_version,
    argMax(meta_consensus_implementation, slot_start_date_time) AS meta_consensus_implementation,
    argMax(meta_consensus_version, slot_start_date_time) AS meta_consensus_version,
    toUInt32(count()) AS slot_count,
    toUInt16(length(groupUniqArrayArray(column_indexes))) AS column_count,
    round(avg(length(column_indexes)), 2) AS avg_columns_per_slot,
    toUInt32(round(quantile(0.5)(first_seen_slot_start_diff))) AS p50_first_seen_slot_start_diff,
    toUInt32(round(quantiles(0.1, 0.5, 0.9, 0.95)(last_seen_slot_start_diff)[1])) AS p10_last_seen_slot_start_diff,
    toUInt32(round(quantiles(0.1, 0.5, 0.9, 0.95)(last_seen_slot_start_diff)[2])) AS p50_last_seen_slot_start_diff,
    toUInt32(round(quantiles(0.1, 0.5, 0.9, 0.95)(last_seen_slot_start_diff)[3])) AS p90_last_seen_slot_start_diff,
    toUInt32(round(quantiles(0.1, 0.5, 0.9, 0.95)(last_seen_slot_start_diff)[4])) AS p95_last_seen_slot_start_diff,
    toUInt32(max(last_seen_slot_start_diff)) AS max_last_seen_slot_start_diff
FROM per_slot
GROUP BY hour_start_date_time, node_id
