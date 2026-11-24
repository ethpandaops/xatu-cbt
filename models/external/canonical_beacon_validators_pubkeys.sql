---
table: canonical_beacon_validators_pubkeys
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: slot # wrong type fix later
---
-- Cannon rewrites every validator's pubkey at every epoch and merges keep
-- one row per validator and value, so this table's own min is a recent epoch that
-- keeps moving, not how far Cannon has processed. A rising min blocks dependents'
-- backfill, so canonical_beacon_validators (every validator, every epoch) bounds
-- are used instead.
SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
    {{ else }}
      toUnixTimestamp(min(epoch_start_date_time)) as min,
    {{ end }}
    toUnixTimestamp(max(epoch_start_date_time)) as max
FROM {{ .self.helpers.from | replace (printf "`%s" .self.table) "`canonical_beacon_validators" }}
WHERE
    meta_network_name = '{{ .env.NETWORK }}'

    -- previous_max if incremental scan and is set, otherwise default/env
    {{- $ts := default (default "0" .env.EXTERNAL_MODEL_MIN_TIMESTAMP) .env.VALIDATORS_MIN_TIMESTAMP -}}
    {{- if .cache.is_incremental_scan -}}
      {{- if .cache.previous_max -}}
        {{- $ts = .cache.previous_max -}}
      {{- end -}}
    {{- end }}
    AND epoch_start_date_time >= fromUnixTimestamp({{ $ts }})
    {{- if .cache.is_incremental_scan }}
      AND epoch_start_date_time <= fromUnixTimestamp({{ $ts }}) + {{ default "100000" .env.EXTERNAL_MODEL_SCAN_SIZE_TIMESTAMP }}
    {{- end }}
