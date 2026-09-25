---
database: observoor
table: memory_usage
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
      toUnixTimestamp(min(wallclock_slot_start_date_time)) as min,
    {{ end }}
    toUnixTimestamp(max(wallclock_slot_start_date_time)) as max
FROM {{ .self.helpers.from }}
WHERE
    meta_network_name = '{{ .env.NETWORK }}'

    -- previous_max if incremental scan and is set, otherwise default/env
    {{- $ts := default "0" .env.EXTERNAL_MODEL_MIN_TIMESTAMP -}}
    {{- if .cache.is_incremental_scan -}}
      {{- if .cache.previous_max -}}
        {{- $ts = .cache.previous_max -}}
      {{- end -}}
    {{- end }}
    AND wallclock_slot_start_date_time >= fromUnixTimestamp({{ $ts }})
    {{- if .cache.is_incremental_scan }}
      AND wallclock_slot_start_date_time <= fromUnixTimestamp({{ $ts }}) + {{ default "100000" .env.EXTERNAL_MODEL_SCAN_SIZE_TIMESTAMP }}
      {{- if .cache.previous_max }}
      -- Also bound window_start (leads the sort/partition key) so the scan is pruned; it trails
      -- wallclock_slot_start_date_time by < 40s. Guarded on previous_max because subtracting
      -- from fromUnixTimestamp(0) wraps DateTime.
      AND window_start BETWEEN fromUnixTimestamp({{ $ts }}) - INTERVAL 1 MINUTE
        AND fromUnixTimestamp({{ $ts }}) + {{ default "100000" .env.EXTERNAL_MODEL_SCAN_SIZE_TIMESTAMP }} + INTERVAL 5 MINUTE
      {{- end }}
    {{- end }}
