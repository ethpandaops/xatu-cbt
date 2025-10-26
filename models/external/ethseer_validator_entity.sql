---
table: ethseer_validator_entity
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
interval:
  type: entity
---
SELECT 
    min(`index`) as min,
    max(`index`) as max
FROM cluster('{remote_cluster}', default.`{{ .self.table }}`)
WHERE 
    meta_network_name = '{{ .self.database }}'
