---
database: mainnet
table: stg_account_last_access
partition: block_number
interval: 10000
schedule: "@every 1m"
backfill:
  enabled: true
  schedule: "@every 1m"
tags:
  - execution
  - account
dependencies:
  - mainnet.canonical_execution_balance_diffs
  - mainnet.canonical_execution_balance_reads
  - mainnet.canonical_execution_contracts
  - mainnet.canonical_execution_nonce_reads
  - mainnet.canonical_execution_storage_diffs
  - mainnet.canonical_execution_storage_reads
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    address,
    max(block_number) AS block_number
FROM (
    SELECT lower(address) as address, block_number FROM mainnet.canonical_execution_nonce_reads 
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM mainnet.canonical_execution_nonce_diffs
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM mainnet.canonical_execution_balance_diffs
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM mainnet.canonical_execution_balance_reads
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(address) as address, block_number FROM mainnet.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(contract_address) as address, block_number FROM mainnet.canonical_execution_storage_reads
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT lower(contract_address) as address, block_number FROM mainnet.canonical_execution_contracts
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
)
GROUP BY address;
