---
table: libp2p_data_column_completed
cache:
  incremental_scan_interval: 5s
  full_scan_interval: 24h
interval:
  type: slot
lag: 12
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
    -- Rows with an unresolved slot carry the unix epoch as their slot start.
    AND slot_start_date_time > toDateTime(0)

    -- previous_max if incremental scan and is set, otherwise default/env
    {{- $ts := default "0" .env.EXTERNAL_MODEL_MIN_TIMESTAMP -}}
    {{- if .cache.is_incremental_scan -}}
      {{- if .cache.previous_max -}}
        {{- $ts = .cache.previous_max -}}
      {{- end -}}
    {{- end }}
    AND slot_start_date_time >= fromUnixTimestamp({{ $ts }})
    {{- if .cache.is_incremental_scan }}
      AND slot_start_date_time <= fromUnixTimestamp({{ $ts }}) + {{ default "100000" .env.EXTERNAL_MODEL_SCAN_SIZE_TIMESTAMP }}
    {{- end }}
