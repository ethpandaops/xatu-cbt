---
table: int_rocketpool_node_event
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
  - "{{external}}.canonical_execution_block"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    multiIf(
        l.topic0 = '0xf773bca07d020a1bc1fdd45ea3db573da547dd27180143afaf075c158a847594', 'node_registered',
        l.topic0 = '0x4e3bcb61bb8e63cb9ed2c46d47eeb6ae847c629e909fbb32b9d17874affb4a89', 'rpl_staked',
        l.topic0 = '0x9947063f70b076145616018b82ed1dd5585e15b7ae0a0b17a8b06bec4c4c31e2', 'rpl_withdrawn',
        'smoothing_pool_state_changed'
    ) AS event_name,
    lower(concat('0x', substring(l.topic1, 27))) AS node_address,
    multiIf(
        l.topic0 IN ('0x4e3bcb61bb8e63cb9ed2c46d47eeb6ae847c629e909fbb32b9d17874affb4a89', '0x9947063f70b076145616018b82ed1dd5585e15b7ae0a0b17a8b06bec4c4c31e2'),
        reinterpretAsUInt256(reverse(unhex(substring(assumeNotNull(l.data), 3, 64)))),
        toUInt256(0)
    ) AS rpl_amount_wei,
    if(
        l.topic0 = '0xed2d3ca39683fb0f50a70ed75c33a19bfe200e529d99e6f7518453b3fc4e9be4'
        AND reinterpretAsUInt256(reverse(unhex(substring(assumeNotNull(l.data), 3, 64)))) = 1,
        true, false
    ) AS smoothing_state,
    l.block_number AS event_block_number,
    b.block_date_time AS event_date_time,
    l.log_index
FROM {{ index .dep "{{external}}" "canonical_execution_logs" "helpers" "from" }} AS l FINAL
GLOBAL INNER JOIN
(
    SELECT block_number, block_date_time
    FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
) AS b ON l.block_number = b.block_number
WHERE l.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND l.meta_network_name = '{{ .env.NETWORK }}'
    AND l.topic0 IN (
        '0xf773bca07d020a1bc1fdd45ea3db573da547dd27180143afaf075c158a847594',
        '0x4e3bcb61bb8e63cb9ed2c46d47eeb6ae847c629e909fbb32b9d17874affb4a89',
        '0x9947063f70b076145616018b82ed1dd5585e15b7ae0a0b17a8b06bec4c4c31e2',
        '0xed2d3ca39683fb0f50a70ed75c33a19bfe200e529d99e6f7518453b3fc4e9be4'
    )
    AND l.topic1 IS NOT NULL;
