---
table: libp2p_rpc_data_column_custody_probe
cache:
  incremental_scan_interval: 5s
  full_scan_interval: 24h
interval:
  type: slot
lag: 24
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
