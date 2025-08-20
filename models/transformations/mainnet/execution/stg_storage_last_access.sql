---
database: mainnet
table: stg_storage_last_access
partition: block_number
interval: 10000
schedule: "@every 10s"
backfill:
  enabled: true
  schedule: "@every 10s"
tags:
  - execution
  - account
  - storage
dependencies:
  - mainnet.canonical_execution_storage_diffs
  - mainnet.canonical_execution_storage_reads
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH all_storage_data AS (
    SELECT
        lower(address) as address,
        slot AS slot_key,
        block_number AS bn,
        transaction_index,
        internal_index,
        to_value AS value
    FROM mainnet.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT
        lower(contract_address) as address,
        slot AS slot_key,
        block_number AS bn,
        transaction_index,
        internal_index,
        value
    FROM mainnet.canonical_execution_storage_reads
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
)
SELECT
    address,
    slot_key AS slot,
    argMax(bn, (bn, transaction_index, internal_index)) AS block_number,
    argMax(value, (bn, transaction_index, internal_index)) AS value
FROM all_storage_data
GROUP BY address, slot_key;
