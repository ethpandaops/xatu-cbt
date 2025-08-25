---
database: mainnet
table: int_address__account_first_access
forwardfill:
  interval: 1000
  schedule: "@every 10s"
backfill:
  interval: 10000
  schedule: "@every 10s"
tags:
  - mainnet
  - address
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
    min(block_number) AS block_number,
    null AS `version`
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
