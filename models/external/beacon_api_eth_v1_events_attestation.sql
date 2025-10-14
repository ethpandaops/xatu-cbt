---
table: beacon_api_eth_v1_events_attestation
cache:
  incremental_scan_interval: 5s
  full_scan_interval: 24h
interval:
  type: slot
lag: 24
---
SELECT
    -- Hardcoded min date for mainnet to avoid querying the full table on full scans.
    {{ if eq .self.database "mainnet" }}
        {{ if .cache.is_incremental_scan }}
          '{{ .cache.previous_min }}' as min,
        {{ else }}
          toUnixTimestamp(min(slot_start_date_time)) as min,
        {{ end }}
    {{ else }}
    toUnixTimestamp(min(slot_start_date_time)) as min,
    {{ end }}
    toUnixTimestamp(max(slot_start_date_time)) as max
-- Use the default database as predicate pushdown does not work with views.
-- This gives 2-3x the performance.
-- Once we move the data into the mainnet database, we no longer need this.
FROM `default`.`{{ .self.table }}`
WHERE 
    meta_network_name = '{{ .self.database }}'

-- Hardcoded min date for mainnet to avoid querying the full table on full scans.
{{ if eq .self.database "mainnet" }}
  {{ if .cache.is_incremental_scan }}
    AND slot_start_date_time >= fromUnixTimestamp({{ .cache.previous_max }})
  {{ else }}
    AND slot_start_date_time > '2025-06-01 00:00:00'
  {{ end }}
{{ else if .cache.is_incremental_scan }}
    AND (
      slot_start_date_time <= fromUnixTimestamp({{ .cache.previous_min }})
      OR slot_start_date_time >= fromUnixTimestamp({{ .cache.previous_max }})
    )
{{ end }}
