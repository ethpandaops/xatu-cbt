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
FROM {{ .self.helpers.from }}
WHERE 
    meta_network_name = '{{ .env.NETWORK }}'
