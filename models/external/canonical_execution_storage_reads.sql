---
table: canonical_execution_storage_reads
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: block
lag: 384
---
SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM {{ .self.helpers.from }}
WHERE 
    meta_network_name = '{{ .env.NETWORK }}'
{{ if .cache.is_incremental_scan }}
    AND (
      block_number <= {{ .cache.previous_min }}
      OR block_number >= {{ .cache.previous_max }}
    )
{{ end }}
