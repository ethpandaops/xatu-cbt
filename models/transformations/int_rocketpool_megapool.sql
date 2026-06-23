---
table: int_rocketpool_megapool
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
  - "{{external}}.canonical_execution_traces"
  - "{{external}}.canonical_execution_logs"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH node_deposits AS (
    SELECT
        transaction_hash,
        lower(concat('0x', substring(topic1, 27))) AS node_operator,
        fromUnixTimestamp(toUInt32(reinterpretAsUInt256(reverse(unhex(substring(assumeNotNull(data),
            multiIf(
                topic0 = '0x93fa5225d22d9e30472233fc2b47735b2b138382fe4884fd10cfca52ba203491', 131,
                topic0 = '0x7aa1a8eb998c779420645fc14513bf058edb347d95c2fc2e6845bdc22f888631', 67,
                195
            ), 64))))) ) AS deposit_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_logs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND address = '0x6b13698c306a297fee1383cdc2c65d63781d2d47'
        AND topic0 IN (
            '0x93fa5225d22d9e30472233fc2b47735b2b138382fe4884fd10cfca52ba203491',
            '0x7aa1a8eb998c779420645fc14513bf058edb347d95c2fc2e6845bdc22f888631',
            '0x512d56e1f791d3bc07b8085104584ec42faefbefed34bbc0b881d8da16a8ebe1'
        )
        AND topic1 IS NOT NULL
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    lower(tr.result_address) AS megapool_address,
    nd.node_operator,
    tr.block_number AS created_block_number,
    nd.deposit_date_time AS created_date_time,
    tr.transaction_hash
FROM {{ index .dep "{{external}}" "canonical_execution_traces" "helpers" "from" }} AS tr FINAL
GLOBAL INNER JOIN node_deposits AS nd ON tr.transaction_hash = nd.transaction_hash
WHERE tr.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND tr.meta_network_name = '{{ .env.NETWORK }}'
    AND tr.action_from = '0xd5bffeaa9f373b9c367132772faa0b88e3f0e38b'
    AND tr.action_type = 'create'
    AND tr.result_address != '';
