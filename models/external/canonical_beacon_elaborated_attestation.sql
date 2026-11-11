---
table: canonical_beacon_elaborated_attestation
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: slot
# Seconds. Two epochs: slot_start_date_time is the attested slot, and an epoch's
# attestations keep landing in the next epoch's blocks, so the epoch below the
# newest one can still be filling while that newest epoch's insert is in flight.
lag: 768
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

