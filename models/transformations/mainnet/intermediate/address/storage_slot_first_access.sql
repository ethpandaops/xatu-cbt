---
database: mainnet
table: int_address__storage_slot_first_access
forwardfill:
  interval: 1000
  schedule: "@every 10s"
backfill:
  interval: 1000
  schedule: "@every 10s"
tags:
  - mainnet
  - address
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
    argMin(bn, (bn, transaction_index, internal_index)) AS block_number,
    argMin(value, (bn, transaction_index, internal_index)) AS value,
    null AS `version`
FROM all_storage_data
GROUP BY address, slot_key;