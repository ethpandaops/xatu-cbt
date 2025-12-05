---
table: int_storage_slot_diff
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
  - "{{external}}.canonical_execution_storage_diffs"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_date_time,
    block_number,
    lower(address) as address,
    slot as slot_key,
    -- Get effective bytes from the FIRST from_value in the block (min transaction_index, internal_index)
    -- Calculate effective bytes from hex strings
    -- '0x00...00ff' -> 1 byte, '0x<32 bytes>' -> 32 bytes, '0x00...00' -> 0 bytes
    toUInt8(
        ceil(
            length(
                trimLeft(
                    substring(
                        argMin(from_value, (transaction_index, internal_index)),
                        3
                    ),
                    '0'
                )
            ) / 2.0
        )
    ) as effective_bytes_from,
    -- Get effective bytes from the LAST to_value in the block (max transaction_index, internal_index)
    toUInt8(
        ceil(
            length(
                trimLeft(
                    substring(
                        argMax(to_value, (transaction_index, internal_index)),
                        3
                    ),
                    '0'
                )
            ) / 2.0
        )
    ) as effective_bytes_to
FROM {{ index .dep "{{external}}" "canonical_execution_storage_diffs" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY block_number, address, slot
HAVING NOT (effective_bytes_from = 0 AND effective_bytes_to = 0)
