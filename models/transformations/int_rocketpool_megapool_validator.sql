---
table: int_rocketpool_megapool_validator
type: incremental
interval:
  type: block
  max: 50000
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
WITH rp_deposit_txns AS (
    SELECT
        transaction_hash,
        lower(concat('0x', substring(topic1, 27))) AS node_operator,
        multiIf(
            topic0 = '0x7aa1a8eb998c779420645fc14513bf058edb347d95c2fc2e6845bdc22f888631', fromUnixTimestamp(toUInt32(reinterpretAsUInt256(reverse(unhex(substring(assumeNotNull(data), 67, 64)))))),
            topic0 = '0x93fa5225d22d9e30472233fc2b47735b2b138382fe4884fd10cfca52ba203491', fromUnixTimestamp(toUInt32(reinterpretAsUInt256(reverse(unhex(substring(assumeNotNull(data), 131, 64)))))),
            fromUnixTimestamp(toUInt32(reinterpretAsUInt256(reverse(unhex(substring(assumeNotNull(data), 195, 64))))))
        ) AS deposit_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_logs" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND address = '0x6b13698c306a297fee1383cdc2c65d63781d2d47'
        AND topic0 IN (
            '0x7aa1a8eb998c779420645fc14513bf058edb347d95c2fc2e6845bdc22f888631',
            '0x93fa5225d22d9e30472233fc2b47735b2b138382fe4884fd10cfca52ba203491',
            '0x512d56e1f791d3bc07b8085104584ec42faefbefed34bbc0b881d8da16a8ebe1'
        )
        AND topic1 IS NOT NULL
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    concat('0x', substring(d.data, 387, 96)) AS validator_pubkey,
    t.node_operator,
    d.block_number AS deposit_block_number,
    t.deposit_date_time,
    d.transaction_hash,
    d.log_index
FROM {{ index .dep "{{external}}" "canonical_execution_logs" "helpers" "from" }} AS d FINAL
GLOBAL INNER JOIN rp_deposit_txns AS t ON d.transaction_hash = t.transaction_hash
WHERE d.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND d.meta_network_name = '{{ .env.NETWORK }}'
    AND d.address = '0x00000000219ab540356cbb839cbe05303d7705fa'
    AND d.topic0 = '0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5';
