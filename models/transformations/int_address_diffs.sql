---
table: int_address_diffs
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
  - "{{external}}.canonical_execution_contracts"
  - "{{external}}.canonical_execution_nonce_diffs"
  - "{{external}}.canonical_execution_storage_diffs"
  - "{{external}}.canonical_execution_transaction"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH get_tx_success AS (
  SELECT  lower(transaction_hash) AS transaction_hash,
  FROM `{{ index .dep "{{external}}" "canonical_execution_transaction" "database" }}`.`canonical_execution_transaction` FINAL
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
  AND success = true
),
all_address_diffs AS (
    SELECT 
      lower(address) AS address, 
      block_number, 
      lower(transaction_hash) AS transaction_hash 
    FROM `{{ index .dep "{{external}}" "canonical_execution_balance_diffs" "database" }}`.`canonical_execution_balance_diffs` FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT 
      lower(address) AS address, 
      block_number, 
      lower(transaction_hash) AS transaction_hash 
    FROM `{{ index .dep "{{external}}" "canonical_execution_storage_diffs" "database" }}`.`canonical_execution_storage_diffs` FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT 
      lower(address) AS address, 
      block_number, 
      lower(transaction_hash) AS transaction_hash 
    FROM `{{ index .dep "{{external}}" "canonical_execution_nonce_diffs" "database" }}`.`canonical_execution_nonce_diffs` FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT 
      lower(contract_address) AS address, 
      block_number, 
      lower(transaction_hash) AS transaction_hash 
    FROM `{{ index .dep "{{external}}" "canonical_execution_contracts" "database" }}`.`canonical_execution_contracts` FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
address_diffs AS (
  SELECT
    ad.address,
    ad.block_number,
    ad.transaction_hash
  FROM all_address_diffs ad
  GLOBAL JOIN get_tx_success g ON ad.transaction_hash = g.transaction_hash
)
SELECT 
    address,
    block_number,
    countDistinct(transaction_hash) AS tx_count
FROM address_diffs
GROUP BY address, block_number;
