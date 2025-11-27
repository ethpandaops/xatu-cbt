---
table: canonical_beacon_committee
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: slot
lag: 384
---
SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
    {{ else }}
      toUnixTimestamp(min(slot_start_date_time)) as min,
    {{ end }}
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM {{ .self.helpers.from }}
WHERE 
    meta_network_name = '{{ .env.NETWORK }}'

{{ if .cache.is_incremental_scan }}
  AND slot_start_date_time >= fromUnixTimestamp({{ .cache.previous_max }})
{{ else }}
  AND slot_start_date_time > fromUnixTimestamp({{ .env.EXTERNAL_MODEL_MIN_TIMESTAMP }})
{{ end }}
