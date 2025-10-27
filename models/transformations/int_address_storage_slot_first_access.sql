---
table: int_address_storage_slot_first_access
type: incremental
interval:
  type: block
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
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    
    UNION ALL
    
    SELECT
        lower(contract_address) as address,
        slot AS slot_key,
        block_number AS bn,
        transaction_index,
        internal_index,
        value
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_reads" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
)
SELECT
    address,
    slot_key AS slot,
    argMin(bn, (bn, transaction_index, internal_index)) AS block_number,
    argMin(value, (bn, transaction_index, internal_index)) AS value,
    null AS `version`
FROM all_storage_data
GROUP BY address, slot_key;
