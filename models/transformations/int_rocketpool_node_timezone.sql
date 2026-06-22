---
table: int_rocketpool_node_timezone
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
  - "{{external}}.canonical_execution_transaction"
  - "{{external}}.canonical_execution_block"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    t.from_address AS node_address,
    unhex(substring(t.input, 139, toUInt32(reinterpretAsUInt256(reverse(unhex(substring(t.input, 75, 64))))) * 2)) AS timezone,
    t.block_number AS set_block_number,
    b.block_date_time AS set_date_time
FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }} AS t FINAL
GLOBAL INNER JOIN
(
    SELECT block_number, block_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
) AS b ON t.block_number = b.block_number
WHERE t.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND t.meta_network_name = '{{ .env.NETWORK }}'
    AND t.to_address IN (
        '0x4477fbf4af5b34e49662d9217681a763ddc0a322',
        '0x89f478e6cc24f052103628f36598d4c14da3d287',
        '0x372236c940f572020c0c0eb1ac7212460e4e5a33',
        '0x2b52479f6ea009907e46fc43e91064d1b92fdc86',
        '0x67cde7af920682a29fcfea1a179ef0f30f48df3e'
    )
    AND substring(t.input, 3, 8) IN ('27c6f43e', 'a7e6e8b3')
    AND t.input IS NOT NULL;
