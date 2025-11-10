---
table: int_address_slots_stat_per_block
type: incremental
interval:
  type: block
  max: 10000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - address
  - storage
  - slots
dependencies:
  - "{{external}}.canonical_execution_transaction"
  - "{{external}}.canonical_execution_storage_diffs"
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
storage_changes AS (
    SELECT
        lower(sd.address) AS address,
        sd.block_number,
        sd.from_value,
        sd.to_value
    FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }} sd FINAL
    GLOBAL JOIN get_tx_success g
        ON lower(sd.transaction_hash) = g.transaction_hash
    WHERE sd.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND sd.meta_network_name = '{{ .env.NETWORK }}'
),
address_slot_stats AS (
    SELECT
        address,
        block_number,
        countIf(from_value != '0x0000000000000000000000000000000000000000000000000000000000000000'
                AND to_value = '0x0000000000000000000000000000000000000000000000000000000000000000') AS slots_cleared,
        countIf(from_value = '0x0000000000000000000000000000000000000000000000000000000000000000'
                AND to_value != '0x0000000000000000000000000000000000000000000000000000000000000000') AS slots_set
    FROM storage_changes
    GROUP BY address, block_number
)
SELECT
    address,
    block_number,
    slots_cleared,
    slots_set,
    NULL AS net_slots,
    NULL AS net_slots_bytes
FROM address_slot_stats;
