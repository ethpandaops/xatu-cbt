---
table: int_storage_slot_read
type: incremental
interval:
  type: block
  max: 100000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5s"
tags:
  - execution
  - storage
dependencies:
  - "{{external}}.canonical_execution_storage_reads"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_date_time,
    block_number,
    lower(contract_address) as address,
    slot as slot_key,
    -- Count how many times this slot was read in the block
    toUInt32(count()) as read_count,
    -- Get effective bytes from the FIRST value read in the block (min transaction_index, internal_index)
    toUInt8(
        ceil(
            length(
                trimLeft(
                    substring(
                        argMin(value, (transaction_index, internal_index)),
                        3
                    ),
                    '0'
                )
            ) / 2.0
        )
    ) as effective_bytes
FROM {{ index .dep "{{external}}" "canonical_execution_storage_reads" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY block_number, contract_address, slot
