---
table: fct_engine_new_payload_by_el_client_hourly
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - hourly
  - engine_api
  - new_payload
  - el_client
dependencies:
  - "{{external}}.consensus_engine_api_new_payload"
---
-- Hourly aggregation of engine_newPayload by execution client.
-- Computes TRUE percentiles (p50, p95) across all observations in each hour per client,
-- enabling accurate percentile comparisons across execution clients.
--
-- This query expands the slot range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. For example, if we process
-- slots spanning 11:46-12:30, we expand to include ALL slots from 11:00-12:59
-- so that hour 11:00 (which was partial in the previous run) gets re-aggregated with
-- complete data. The ReplacingMergeTree will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries from the data within the current bounds
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{external}}" "consensus_engine_api_new_payload" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),
    -- Find ALL events that fall within those hour boundaries (expanding to complete hours)
    events_in_hours AS (
        SELECT
            slot_start_date_time,
            block_hash,
            duration_ms,
            status,
            gas_used,
            gas_limit,
            tx_count,
            blob_count,
            meta_execution_implementation,
            meta_execution_version,
            meta_client_name,
            CASE
                WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870-block-builder'
                ELSE ''
            END AS node_class
        FROM {{ index .dep "{{external}}" "consensus_engine_api_new_payload" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND meta_execution_implementation != ''
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    meta_execution_implementation,
    meta_execution_version,
    node_class,
    count(DISTINCT slot_start_date_time, block_hash) AS slot_count,
    count(*) AS observation_count,
    count(DISTINCT meta_client_name) AS unique_node_count,
    -- Status counts (for VALID-only filtering in queries)
    countIf(status = 'VALID') AS valid_count,
    countIf(status = 'INVALID') AS invalid_count,
    countIf(status = 'SYNCING') AS syncing_count,
    countIf(status = 'ACCEPTED') AS accepted_count,
    -- Duration statistics (VALID status only) - TRUE percentiles
    ifNotFinite(round(avgIf(duration_ms, status = 'VALID')), 0) AS avg_duration_ms,
    ifNotFinite(round(quantileIf(0.5)(duration_ms, status = 'VALID')), 0) AS p50_duration_ms,
    ifNotFinite(round(quantileIf(0.95)(duration_ms, status = 'VALID')), 0) AS p95_duration_ms,
    ifNotFinite(minIf(duration_ms, status = 'VALID'), 0) AS min_duration_ms,
    ifNotFinite(maxIf(duration_ms, status = 'VALID'), 0) AS max_duration_ms,
    -- Block complexity metrics (VALID status only)
    ifNotFinite(round(avgIf(gas_used, status = 'VALID')), 0) AS avg_gas_used,
    ifNotFinite(round(avgIf(gas_limit, status = 'VALID')), 0) AS avg_gas_limit,
    ifNotFinite(round(avgIf(tx_count, status = 'VALID'), 1), 0) AS avg_tx_count,
    ifNotFinite(round(avgIf(blob_count, status = 'VALID'), 1), 0) AS avg_blob_count
FROM events_in_hours
GROUP BY
    toStartOfHour(slot_start_date_time),
    meta_execution_implementation,
    meta_execution_version,
    node_class
