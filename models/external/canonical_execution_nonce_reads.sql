---
table: canonical_execution_nonce_reads
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: block
lag: 384
---
SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
    {{ else }}
      min(block_number) as min,
    {{ end }}
    max(block_number) as max
FROM {{ .self.helpers.from }}
WHERE 
    meta_network_name = '{{ .env.NETWORK }}'

{{ if .cache.is_incremental_scan }}
  AND block_number >= {{ .cache.previous_max }}
{{ else }}
  AND block_number > {{ .env.EXTERNAL_MODEL_MIN_BLOCK }}
{{ end }}
