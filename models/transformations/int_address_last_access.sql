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
  - "{{external}}.canonical_execution_transaction"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH get_tx_success AS (
  SELECT lower(transaction_hash) AS transaction_hash
  FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }} FINAL
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND success = true
    AND meta_network_name = '{{ .env.NETWORK }}'
),
all_addresses AS (
    SELECT 
      lower(address) AS address, 
      lower(transaction_hash) AS transaction_hash, 
      block_number 
    FROM {{ index .dep "{{external}}" "canonical_execution_nonce_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT 
      lower(address) AS address, 
      lower(transaction_hash) AS transaction_hash, 
      block_number 
    FROM {{ index .dep "{{external}}" "canonical_execution_nonce_diffs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT 
      lower(address) AS address, 
      lower(transaction_hash) AS transaction_hash, 
      block_number 
    FROM {{ index .dep "{{external}}" "canonical_execution_balance_diffs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT 
      lower(address) AS address, 
      lower(transaction_hash) AS transaction_hash, 
      block_number 
    FROM {{ index .dep "{{external}}" "canonical_execution_balance_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT 
      lower(address) AS address, 
      lower(transaction_hash) AS transaction_hash, 
      block_number 
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT 
      lower(contract_address) AS address, 
      lower(transaction_hash) AS transaction_hash, 
      block_number 
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT 
      lower(contract_address) AS address, 
      lower(transaction_hash) AS transaction_hash, 
      block_number 
    FROM {{ index .dep "{{external}}" "canonical_execution_contracts" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
)
SELECT 
    a.address,
    max(a.block_number) AS block_number
FROM all_addresses a
GLOBAL JOIN get_tx_success g ON a.transaction_hash = g.transaction_hash
GROUP BY a.address;