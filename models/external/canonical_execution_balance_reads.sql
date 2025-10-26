---
table: canonical_execution_balance_reads
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
-- Use the default database as predicate pushdown does not work with views.
-- This gives 2-3x the performance.
-- Once we move the data into the mainnet database, we no longer need this.
FROM cluster('{remote_cluster}', default.`{{ .self.table }}`)
WHERE 
    meta_network_name = '{{ .self.database }}'
{{ if .cache.is_incremental_scan }}
    AND (
      block_number <= {{ .cache.previous_min }}
      OR block_number >= {{ .cache.previous_max }}
    )
{{ end }}
