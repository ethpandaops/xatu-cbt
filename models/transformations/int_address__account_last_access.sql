---
table: int_address__account_last_access
interval:
  max: 10000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - address
  - account
dependencies:
  - "{{external}}.canonical_execution_balance_diffs"
  - "{{external}}.canonical_execution_balance_reads"
  - "{{external}}.canonical_execution_contracts"
  - "{{external}}.canonical_execution_nonce_reads"
  - "{{external}}.canonical_execution_nonce_diffs"
  - "{{external}}.canonical_execution_storage_diffs"
  - "{{external}}.canonical_execution_storage_reads"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    address,
    max(block_number) AS block_number
FROM (
    SELECT lower(address) as address, block_number FROM `{{ index .dep "{{external}}" "canonical_execution_nonce_reads" "database" }}`.`canonical_execution_nonce_reads `
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM `{{ index .dep "{{external}}" "canonical_execution_nonce_diffs" "database" }}`.`canonical_execution_nonce_diffs`
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM `{{ index .dep "{{external}}" "canonical_execution_balance_diffs" "database" }}`.`canonical_execution_balance_diffs`
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM `{{ index .dep "{{external}}" "canonical_execution_balance_reads" "database" }}`.`canonical_execution_balance_reads`
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM `{{ index .dep "{{external}}" "canonical_execution_storage_diffs" "database" }}`.`canonical_execution_storage_diffs`
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(contract_address) as address, block_number FROM `{{ index .dep "{{external}}" "canonical_execution_storage_reads" "database" }}`.`canonical_execution_storage_reads`
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(contract_address) as address, block_number FROM `{{ index .dep "{{external}}" "canonical_execution_contracts" "database" }}`.`canonical_execution_contracts`
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
)
GROUP BY address;
