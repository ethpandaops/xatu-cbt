---
table: libp2p_handle_status
cache:
  incremental_scan_interval: 5s
  full_scan_interval: 24h
interval:
  type: datetime
  lag: 60
---
SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
    {{ else }}
      toUnixTimestamp(min(event_date_time)) as min,
    {{ end }}
    toUnixTimestamp(max(event_date_time)) as max
FROM {{ .self.helpers.from }}
WHERE
    meta_network_name = '{{ .env.NETWORK }}'

{{ if .cache.is_incremental_scan }}
  AND event_date_time >= fromUnixTimestamp({{ .cache.previous_max }})
{{ else }}
  AND event_date_time >= fromUnixTimestamp({{ default "0" .env.EXTERNAL_MODEL_MIN_TIMESTAMP }})
{{ end }}
