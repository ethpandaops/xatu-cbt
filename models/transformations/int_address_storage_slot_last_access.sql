---
table: int_address_storage_slot_last_access
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
  - "{{external}}.canonical_execution_transaction"
  - "{{external}}.canonical_execution_storage_diffs"
  - "{{external}}.canonical_execution_storage_reads"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
get_tx_success AS (
  SELECT lower(transaction_hash) AS transaction_hash
  FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }} FINAL
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND success = true
    AND meta_network_name = '{{ .env.NETWORK }}'
),
all_storage_data AS (
  SELECT
    lower(address) AS address,
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
    lower(sr.contract_address) AS address,
    sr.slot AS slot_key,
    sr.block_number AS bn,
    0 AS transaction_index,
    0 AS internal_index,
    sr.value
  FROM {{ index .dep "{{external}}" "canonical_execution_storage_reads" "helpers" "from" }} sr FINAL
  GLOBAL JOIN get_tx_success g
    ON lower(sr.transaction_hash) = g.transaction_hash
  WHERE sr.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
)
SELECT
  address,
  slot_key AS slot,
  argMax(bn, (bn, transaction_index, internal_index)) AS block_number,
  argMax(value, (bn, transaction_index, internal_index)) AS value
FROM all_storage_data
GROUP BY address, slot_key;
