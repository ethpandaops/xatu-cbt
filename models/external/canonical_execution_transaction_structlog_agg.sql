---
table: canonical_execution_transaction_structlog_agg
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: block
lag: 384
---
SELECT
  {{ if .cache.is_incremental_scan }}
  greatest({{ .cache.previous_min }}, {{ default "0" .env.STRUCTLOG_AGG_MIN_BLOCK }}) as min,
  {{ else }}
  greatest(min(block_number), {{ default "0" .env.STRUCTLOG_AGG_MIN_BLOCK }}) as min,
  {{ end }}
    max(block_number) as max
FROM {{ .self.helpers.from }}
WHERE
    meta_network_name = '{{ .env.NETWORK }}'

    -- previous_max if incremental scan and is set, otherwise default/env
    {{- $bn := default "0" .env.EXTERNAL_MODEL_MIN_BLOCK -}}
    {{- if .cache.is_incremental_scan -}}
      {{- if .cache.previous_max -}}
        {{- $bn = .cache.previous_max -}}
      {{- end -}}
    {{- end }}
    AND block_number >= {{ $bn }}
    {{- if .cache.is_incremental_scan }}
      AND block_number <= {{ $bn }} + {{ default "10000" .env.EXTERNAL_MODEL_SCAN_SIZE_BLOCK }}
    {{- end }}
