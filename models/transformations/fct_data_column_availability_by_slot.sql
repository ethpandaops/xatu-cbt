---
table: fct_data_column_availability_by_slot
type: scheduled
schedule: "@every 1h"
tags:
  - slot
  - data_column
  - peerdas
  - custody
dependencies:
  - "{{transformation}}.int_custody_probe_order_by_slot"
  - "{{transformation}}.int_data_column_gossip_by_slot"
---
-- Custody probe results per slot and column. Probes test slots up to about 18 days old, so every run recounts the
-- slots of the last DATA_COLUMN_AVAILABILITY_LOOKBACK_DAYS days (default 19) and today. Rows older than that keep
-- their last count, which no longer changes. Each statement covers DATA_COLUMN_AVAILABILITY_CHUNK_DAYS days
-- (default 1) to bound memory.
--
-- Only custody probes count towards success, failure, missing and availability. Gossipsub sightings are kept
-- alongside in gossipsub_count, with the block root they carried: a peer gossiping a column says nothing about
-- whether it custodies the column when asked.
{{- $days := atoi (default "19" .env.DATA_COLUMN_AVAILABILITY_LOOKBACK_DAYS) }}
{{- $chunk := atoi (default "1" .env.DATA_COLUMN_AVAILABILITY_CHUNK_DAYS) }}
{{- $chunks := int (div (add $days $chunk) $chunk) }}
{{- range $i := until $chunks }}
INSERT INTO `{{ $.self.database }}`.`{{ $.self.table }}`
WITH
    toStartOfDay(fromUnixTimestamp({{ $.task.start }})) + INTERVAL 1 DAY - INTERVAL {{ mul (add $i 1) $chunk }} DAY AS chunk_start,
    toStartOfDay(fromUnixTimestamp({{ $.task.start }})) + INTERVAL 1 DAY - INTERVAL {{ mul $i $chunk }} DAY AS chunk_end,
probes AS (
    SELECT
        slot,
        slot_start_date_time,
        toUInt32(slot DIV 32) AS epoch,
        toDateTime(toUnixTimestamp(slot_start_date_time) - ((slot % 32) * 12)) AS epoch_start_date_time,
        column_index,
        max(toUInt16(length(blob_submitters))) AS blob_count,
        toUInt32(sumIf(1, result = 'success')) AS success_count,
        toUInt32(sumIf(1, result = 'failure')) AS failure_count,
        toUInt32(sumIf(1, result = 'missing')) AS missing_count,
        toUInt32(count()) AS probe_count,
        if(isFinite(minIf(response_time_ms, result = 'success')), toUInt32(round(minIf(response_time_ms, result = 'success'))), 0) AS min_response_time_ms,
        quantilesIf(0.50, 0.95, 0.99)(response_time_ms, result = 'success') AS response_quantiles,
        if(isFinite(maxIf(response_time_ms, result = 'success')), toUInt32(round(maxIf(response_time_ms, result = 'success'))), 0) AS max_response_time_ms,
        toUInt32(uniqExact(peer_id_unique_key)) AS unique_peer_count,
        toUInt32(uniqExact(username)) AS unique_client_count,
        toUInt32(uniqExact(meta_client_implementation)) AS unique_implementation_count
    FROM {{ index $.dep "{{transformation}}" "int_custody_probe_order_by_slot" "helpers" "from" }} FINAL
    ARRAY JOIN column_indices AS column_index
    WHERE slot_start_date_time >= chunk_start AND slot_start_date_time < chunk_end
    GROUP BY slot, slot_start_date_time, column_index
),
gossip AS (
    SELECT slot_start_date_time, column_index, beacon_block_root, beacon_block_root_variants, blob_count, gossipsub_count
    FROM {{ index $.dep "{{transformation}}" "int_data_column_gossip_by_slot" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= chunk_start AND slot_start_date_time < chunk_end
)
SELECT
    fromUnixTimestamp({{ $.task.start }}) AS updated_date_time,
    p.slot AS slot,
    p.slot_start_date_time AS slot_start_date_time,
    p.epoch AS epoch,
    p.epoch_start_date_time AS epoch_start_date_time,
    -- Probes are counted against the slot they tested
    p.slot AS wallclock_request_slot,
    p.slot_start_date_time AS wallclock_request_slot_start_date_time,
    p.epoch AS wallclock_request_epoch,
    p.epoch_start_date_time AS wallclock_request_epoch_start_date_time,
    p.column_index AS column_index,
    nullIf(g.beacon_block_root, '') AS beacon_block_root,
    g.beacon_block_root_variants AS beacon_block_root_variants,
    greatest(p.blob_count, g.blob_count) AS blob_count,
    p.success_count AS success_count,
    p.failure_count AS failure_count,
    p.missing_count AS missing_count,
    p.probe_count AS probe_count,
    round(p.success_count * 100.0 / p.probe_count, 2) AS availability_pct,
    p.min_response_time_ms AS min_response_time_ms,
    if(isFinite(p.response_quantiles[1]), toUInt32(round(p.response_quantiles[1])), 0) AS p50_response_time_ms,
    if(isFinite(p.response_quantiles[2]), toUInt32(round(p.response_quantiles[2])), 0) AS p95_response_time_ms,
    if(isFinite(p.response_quantiles[3]), toUInt32(round(p.response_quantiles[3])), 0) AS p99_response_time_ms,
    p.max_response_time_ms AS max_response_time_ms,
    p.unique_peer_count AS unique_peer_count,
    p.unique_client_count AS unique_client_count,
    p.unique_implementation_count AS unique_implementation_count,
    p.probe_count AS custody_probe_count,
    g.gossipsub_count AS gossipsub_count
FROM probes AS p
LEFT JOIN gossip AS g ON p.slot_start_date_time = g.slot_start_date_time AND p.column_index = g.column_index
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0;
{{- end }}
