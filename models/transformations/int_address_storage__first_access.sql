---
table: int_address_storage__first_access
interval:
  max: 1000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - address
  - storage
dependencies:
  - "{{external}}.canonical_execution_storage_diffs"
  - "{{external}}.canonical_execution_storage_reads"
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
    FROM `{{ index .dep "{{external}}" "canonical_execution_storage_diffs" "database" }}`.`canonical_execution_storage_diffs` FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    
    UNION ALL
    
    SELECT
        lower(contract_address) as address,
        slot AS slot_key,
        block_number AS bn,
        transaction_index,
        internal_index,
        value
    FROM `{{ index .dep "{{external}}" "canonical_execution_storage_reads" "database" }}`.`canonical_execution_storage_reads` FINAL
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
