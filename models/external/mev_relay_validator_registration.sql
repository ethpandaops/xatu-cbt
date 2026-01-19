---
table: mev_relay_validator_registration
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: slot
lag: 12
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

    -- previous_max if incremental scan and is set, otherwise default/env
    {{- $ts := default (default "0" .env.EXTERNAL_MODEL_MIN_TIMESTAMP) .env.EXTERNAL_MODEL_MIN_TIMESTAMP_EXTRA -}}
    {{- if .cache.is_incremental_scan -}}
      {{- if .cache.previous_max -}}
        {{- $ts = .cache.previous_max -}}
      {{- end -}}
    {{- end }}
    AND event_date_time >= fromUnixTimestamp({{ $ts }})
    {{- if .cache.is_incremental_scan }}
      AND event_date_time <= fromUnixTimestamp({{ $ts }}) + {{ default "100000" .env.EXTERNAL_MODEL_SCAN_SIZE_TIMESTAMP }}
    {{- end }}

