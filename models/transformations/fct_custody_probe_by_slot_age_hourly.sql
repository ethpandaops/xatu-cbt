---
table: fct_custody_probe_by_slot_age_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 1m"
tags:
  - hourly
  - data_column
  - peerdas
  - custody
  - probe
dependencies:
  - "{{transformation}}.int_custody_probe"
---
-- Data column custody probes by when they ran, the age of the slot they tested and the probed P2P peer's client.
-- Probes test slots across the whole retention window, so late buckets show peers that stop custodying columns
-- before retention ends. Counts are column probes, one per column a probe asked for.
-- Rewrites every hour the bounds touch so partial hours are recounted in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    bounds AS (
        SELECT
            toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_bucket,
            toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 HOUR AS max_bucket
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(probe_date_time) AS hour_start_date_time,
    -- Age of the probed slot when the probe ran. Beacon nodes keep data columns for 4096 epochs (about 18.2 days).
    multiIf(
        age < 86400, '0-1d',
        age < 3 * 86400, '1-3d',
        age < 7 * 86400, '3-7d',
        age < 14 * 86400, '7-14d',
        age < 18 * 86400, '14-18d',
        '18d+'
    ) AS slot_age_bucket,
    toUInt8(multiIf(age < 86400, 0, age < 3 * 86400, 1, age < 7 * 86400, 3, age < 14 * 86400, 7, age < 18 * 86400, 14, 18)) AS slot_age_min_days,
    client,
    toUInt64(sum(columns)) AS probe_count,
    toUInt64(sumIf(columns, result = 'success')) AS success_count,
    toUInt64(sumIf(columns, result = 'missing')) AS missing_count,
    toUInt64(sumIf(columns, result = 'failure')) AS failure_count
FROM (
    SELECT
        probe_date_time,
        greatest(toInt64(toUnixTimestamp(probe_date_time)) - toInt64(toUnixTimestamp(slot_start_date_time)), 0) AS age,
        length(column_indices) AS columns,
        result,
        multiIf(
            lower(meta_peer_implementation) = '', 'unknown',
            lower(meta_peer_implementation) = 'erigon', 'caplin',
            match(lower(meta_peer_implementation), '^[a-z][a-z0-9-]*$'), lower(meta_peer_implementation),
            'other'
        ) AS client
    FROM {{ index .dep "{{transformation}}" "int_custody_probe" "helpers" "from" }} FINAL
    WHERE probe_date_time >= (SELECT min_bucket FROM bounds)
        AND probe_date_time < (SELECT max_bucket FROM bounds)
        -- Crawlers and other probers are not custody nodes, as in fct_custody_probe_by_client_daily
        AND NOT (lower(meta_peer_implementation) IN ('tysm', 'hermes', 'nebula', 'eth2-crawler', 'enrscout', 'oft-probe', 'migalabs-kumo', 'xrpc-basel1lc', 'geth-blsync', 'nimbus-verified-proxy')
            OR match(lower(meta_peer_implementation), 'crawler|probe|scout|armiarma'))
)
GROUP BY hour_start_date_time, slot_age_bucket, slot_age_min_days, client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
