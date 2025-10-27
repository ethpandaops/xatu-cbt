---
table: int_address_last_access
type: incremental
interval:
  type: block
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
    SELECT lower(address) as address, block_number FROM {{ index .dep "{{external}}" "canonical_execution_nonce_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM {{ index .dep "{{external}}" "canonical_execution_nonce_diffs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM {{ index .dep "{{external}}" "canonical_execution_balance_diffs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM {{ index .dep "{{external}}" "canonical_execution_balance_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT lower(contract_address) as address, block_number FROM {{ index .dep "{{external}}" "canonical_execution_storage_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT lower(contract_address) as address, block_number FROM {{ index .dep "{{external}}" "canonical_execution_contracts" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
)
GROUP BY address;
