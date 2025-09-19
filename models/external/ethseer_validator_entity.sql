---
table: ethseer_validator_entity
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 24h
---
SELECT 
    min(`index`) as min,
    max(`index`) as max
FROM `{{ .self.database }}`.`{{ .self.table }}`
WHERE 
    meta_network_name = '{{ .self.database }}'
