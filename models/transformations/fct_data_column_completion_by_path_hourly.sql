---
table: fct_data_column_completion_by_path_hourly
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
  - partial_messages
dependencies:
  - "{{external}}.libp2p_data_column_completed"
---
-- Rewrites every hour the bounds touch so partial hours are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
hour_bounds AS (
    SELECT
        toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_hour,
        toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 HOUR AS max_hour
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    multiIf(
        position(meta_client_implementation, 'tysm') > 0, 'prysm',
        position(meta_client_implementation, 'temu') > 0, 'teku',
        position(meta_client_implementation, 'dimhouse') > 0, 'lighthouse',
        lower(extract(meta_client_implementation, '\\(([A-Za-z]+)\\)'))
    ) AS meta_consensus_implementation,
    if(completed_via IN ('FULL_GOSSIP', 'PARTIAL_GOSSIP', 'EL', 'RECONSTRUCTION'), completed_via, 'UNKNOWN') AS completed_via,
    toUInt32(uniqExact(meta_client_name)) AS node_count,
    toUInt32(uniqExact(beacon_block_root)) AS block_count,
    toUInt32(count()) AS completion_count,
    toUInt64(sum(cells_total)) AS cells_total,
    toUInt64(sum(cells_from_gossip)) AS cells_from_gossip,
    toUInt64(sum(cells_from_el)) AS cells_from_el,
    round(avg(contributing_peers), 2) AS avg_contributing_peers,
    toUInt32(min(propagation_slot_start_diff)) AS min_completion_slot_start_diff,
    toUInt32(round(quantile(0.5)(propagation_slot_start_diff))) AS p50_completion_slot_start_diff,
    toUInt32(round(quantile(0.9)(propagation_slot_start_diff))) AS p90_completion_slot_start_diff,
    toUInt32(round(quantile(0.99)(propagation_slot_start_diff))) AS p99_completion_slot_start_diff,
    toUInt32(max(propagation_slot_start_diff)) AS max_completion_slot_start_diff
FROM {{ index .dep "{{external}}" "libp2p_data_column_completed" "helpers" "from" }} FINAL
WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
    AND slot_start_date_time < (SELECT max_hour FROM hour_bounds)
    AND beacon_block_root != ''
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY hour_start_date_time, meta_consensus_implementation, completed_via
