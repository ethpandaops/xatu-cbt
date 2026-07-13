---
table: fct_transaction_replacement
type: incremental
interval:
  type: slot
  max: 21600
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
fill:
  buffer: 612000
tags:
  - slot
  - transaction
  - mempool
  - execution
dependencies:
  - "{{transformation}}.int_transaction_mempool_observation_hourly"
  - "{{transformation}}.fct_transaction_inclusion"
---
-- One row per publicly observed attempt in nonce groups with at least two attempts,
-- emitted when the group resolves. A nonce group is all transactions sharing a from
-- address and nonce whose first sightings fall within 7 days of the group anchor
-- (the earliest attempt sighting). The fill buffer keeps this model 612000 seconds
-- behind its dependencies so the full group window exists before a chunk is processed.
-- Attempts are ordered deterministically by (first sighting, hash).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH obs AS (
    SELECT
        hash,
        first_seen_date_time,
        `from`,
        `to`,
        nonce,
        type,
        gas_price,
        gas_tip_cap,
        gas_fee_cap,
        value,
        is_cancel_shape
    FROM {{ index .dep "{{transformation}}" "int_transaction_mempool_observation_hourly" "helpers" "from" }} FINAL
    WHERE hour_start_date_time BETWEEN toStartOfHour(fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 7 DAY)
        AND toStartOfHour(fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 7 DAY)
),
-- earliest attempt sighting per nonce group within the anchor window
anchor_groups AS (
    SELECT
        `from`,
        nonce,
        min(first_seen_date_time) AS gfs
    FROM obs
    WHERE first_seen_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY `from`, nonce
),
-- keep only groups with no attempt sighting in the 7 days before their anchor
group_episodes AS (
    SELECT
        a.`from` AS `from`,
        a.nonce AS nonce,
        a.gfs AS gfs
    FROM anchor_groups a
    GLOBAL LEFT JOIN obs o ON o.`from` = a.`from` AND o.nonce = a.nonce
    GROUP BY a.`from`, a.nonce, a.gfs
    HAVING countIf(o.first_seen_date_time < a.gfs
        AND o.first_seen_date_time >= a.gfs - INTERVAL 7 DAY) = 0
),
-- one row per attempt hash within the group window
attempts AS (
    SELECT
        g.`from` AS `from`,
        g.nonce AS nonce,
        g.gfs AS gfs,
        o.hash AS hash,
        min(o.first_seen_date_time) AS afs,
        argMin(o.`to`, o.first_seen_date_time) AS `to`,
        argMin(o.type, o.first_seen_date_time) AS type,
        argMin(o.gas_price, o.first_seen_date_time) AS gas_price,
        argMin(o.gas_tip_cap, o.first_seen_date_time) AS gas_tip_cap,
        argMin(o.gas_fee_cap, o.first_seen_date_time) AS gas_fee_cap,
        argMin(o.value, o.first_seen_date_time) AS value,
        argMin(o.is_cancel_shape, o.first_seen_date_time) AS is_cancel_shape
    FROM group_episodes g
    GLOBAL INNER JOIN obs o ON o.`from` = g.`from` AND o.nonce = g.nonce
    WHERE o.first_seen_date_time >= g.gfs
        AND o.first_seen_date_time <= g.gfs + INTERVAL 7 DAY
    GROUP BY g.`from`, g.nonce, g.gfs, o.hash
),
-- nonce groups with at least two observed attempts
multi_groups AS (
    SELECT
        `from`,
        nonce
    FROM attempts
    GROUP BY `from`, nonce
    HAVING uniqExact(hash) >= 2
),
-- inclusions of the qualifying groups, wide enough to catch consumption before sighting
incl AS (
    SELECT
        hash,
        `from`,
        nonce,
        slot_start_date_time
    FROM {{ index .dep "{{transformation}}" "fct_transaction_inclusion" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 14 DAY
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 7 DAY
        AND (`from`, nonce) GLOBAL IN (SELECT `from`, nonce FROM multi_groups)
),
-- earliest inclusion of each group within the group window
winners AS (
    SELECT
        g.`from` AS `from`,
        g.nonce AS nonce,
        argMin(i.hash, i.slot_start_date_time) AS whash,
        min(i.slot_start_date_time) AS wslot
    FROM group_episodes g
    GLOBAL INNER JOIN incl i ON i.`from` = g.`from` AND i.nonce = g.nonce
    WHERE i.slot_start_date_time <= g.gfs + INTERVAL 7 DAY
    GROUP BY g.`from`, g.nonce
),
indexed AS (
    SELECT
        a.`from` AS `from`,
        a.nonce AS nonce,
        a.gfs AS gfs,
        a.hash AS hash,
        a.afs AS afs,
        a.`to` AS `to`,
        a.type AS type,
        a.gas_price AS gas_price,
        a.gas_tip_cap AS gas_tip_cap,
        a.gas_fee_cap AS gas_fee_cap,
        a.value AS value,
        a.is_cancel_shape AS is_cancel_shape,
        toUInt16(row_number() OVER w) AS attempt_index,
        toUInt16(count(*) OVER (PARTITION BY a.`from`, a.nonce)) AS group_attempt_count,
        lagInFrame(a.hash) OVER w AS prev_hash_raw,
        lagInFrame(a.gas_tip_cap) OVER w AS prev_gas_tip_cap,
        lagInFrame(a.gas_fee_cap) OVER w AS prev_gas_fee_cap
    FROM attempts a
    WHERE (a.`from`, a.nonce) GLOBAL IN (SELECT `from`, nonce FROM multi_groups)
    WINDOW w AS (PARTITION BY a.`from`, a.nonce ORDER BY a.afs ASC, a.hash ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    x.gfs AS group_first_seen_date_time,
    x.`from` AS `from`,
    x.nonce AS nonce,
    x.hash AS hash,
    x.attempt_index AS attempt_index,
    x.group_attempt_count AS group_attempt_count,
    x.afs AS first_seen_date_time,
    if(x.attempt_index = 1, NULL, x.prev_hash_raw) AS previous_hash,
    x.`to` AS `to`,
    x.type AS type,
    x.gas_price AS gas_price,
    x.gas_tip_cap AS gas_tip_cap,
    x.gas_fee_cap AS gas_fee_cap,
    x.value AS value,
    if(x.attempt_index = 1, NULL, toInt128(x.gas_tip_cap) - toInt128(x.prev_gas_tip_cap)) AS gas_tip_cap_delta,
    if(x.attempt_index = 1, NULL, toInt128(x.gas_fee_cap) - toInt128(x.prev_gas_fee_cap)) AS gas_fee_cap_delta,
    x.is_cancel_shape AS is_cancel_shape,
    ifNull(x.hash = wn.whash, 0) AS is_winner,
    if(wn.wslot IS NOT NULL, 'included', 'unincluded') AS group_outcome,
    wn.whash AS winner_hash,
    multiIf(
        wn.wslot IS NOT NULL, toDateTime64(wn.wslot, 3),
        x.gfs + INTERVAL 7 DAY
    ) AS resolution_date_time,
    x.afs > resolution_date_time AS observed_after_resolution
FROM indexed x
GLOBAL LEFT JOIN winners wn ON wn.`from` = x.`from` AND wn.nonce = x.nonce
SETTINGS join_use_nulls = 1
