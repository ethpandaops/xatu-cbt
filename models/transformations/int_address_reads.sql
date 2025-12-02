---
table: int_address_reads
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
  - "{{external}}.canonical_execution_balance_reads"
  - "{{external}}.canonical_execution_nonce_reads"
  - "{{external}}.canonical_execution_storage_reads"
  - "{{external}}.canonical_execution_transaction"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH get_tx_success AS (
  SELECT
    lower(transaction_hash) AS transaction_hash,
    transaction_index
  FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }} FINAL
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND success = true
    AND meta_network_name = '{{ .env.NETWORK }}'
),
all_address_reads AS (
    SELECT 
      lower(address) AS address, 
      block_number, 
      lower(transaction_hash) AS transaction_hash 
    FROM {{ index .dep "{{external}}" "canonical_execution_balance_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    UNION ALL
    
    SELECT 
      lower(contract_address) AS address, 
      block_number, 
      lower(transaction_hash) AS transaction_hash 
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    UNION ALL
    
    SELECT 
      lower(address) AS address, 
      block_number, 
      lower(transaction_hash) AS transaction_hash 
    FROM {{ index .dep "{{external}}" "canonical_execution_nonce_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
),
address_reads AS (
  SELECT
    ar.address,
    ar.block_number,
    ar.transaction_hash,
    g.transaction_index
  FROM all_address_reads ar
  GLOBAL JOIN get_tx_success g ON ar.transaction_hash = g.transaction_hash
)
SELECT 
    address,
    block_number,
    countDistinct(transaction_hash) AS tx_count,
    max(transaction_index)
FROM address_reads
GROUP BY address, block_number;
