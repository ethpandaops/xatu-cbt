---
database: mainnet
table: canonical_execution_contracts
cache:
  incremental_scan_interval: 30s
  full_scan_interval: 24h
---
SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM `{{ .self.database }}`.`{{ .self.table }}`
{{ if .cache.is_incremental_scan }}
WHERE block_number <= {{ .cache.previous_min }}
   OR block_number >= {{ .cache.previous_max }}
{{ end }}
