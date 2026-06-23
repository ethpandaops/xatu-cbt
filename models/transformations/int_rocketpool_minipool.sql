---
table: int_rocketpool_minipool
type: incremental
interval:
  type: block
  max: 50000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 30s"
tags:
  - execution
  - rocketpool
dependencies:
  - "{{external}}.canonical_execution_logs"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    multiIf(
        topic0 = '0x08b4b91bafaf992145c5dd7e098dfcdb32f879714c154c651c2758a44c7aeae4', 'created',
        'destroyed'
    ) AS event_name,
    lower(concat('0x', substring(topic1, 27))) AS minipool_address,
    lower(concat('0x', substring(topic2, 27))) AS node_operator,
    block_number AS event_block_number,
    fromUnixTimestamp(toUInt32(reinterpretAsUInt256(reverse(unhex(substring(assumeNotNull(data), 3, 64)))))) AS event_date_time,
    transaction_hash,
    log_index
FROM {{ index .dep "{{external}}" "canonical_execution_logs" "helpers" "from" }} FINAL
WHERE
    block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND topic0 IN (
        '0x08b4b91bafaf992145c5dd7e098dfcdb32f879714c154c651c2758a44c7aeae4',
        '0x3097cb0f536cd88115b814915d7030d2fe958943357cd2b1a9e1dba8a673ec69'
    )
    AND topic1 IS NOT NULL
    AND topic2 IS NOT NULL;
