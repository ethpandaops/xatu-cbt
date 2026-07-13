---
table: fct_transaction_mempool_outcome_7d
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
  - "{{transformation}}.fct_block_mev"
---
-- One immutable row per transaction hash observed in the public mempool, stating its
-- fixed-horizon outcome within 7 days of first sighting. The fill buffer keeps this
-- model 612000 seconds (7 days plus slack) behind its dependencies so the full horizon
-- of observation and inclusion data exists before a chunk is processed.
-- An observation episode starts when a hash is sighted with no sightings in the
-- preceding 7 days. The outcome statement is fixed to the horizon and remains true
-- even if the transaction is included later.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH obs AS (
    SELECT
        hash,
        first_seen_date_time,
        last_seen_date_time,
        sighting_count,
        unique_sentries,
        `from`,
        `to`,
        nonce,
        type,
        gas,
        gas_price,
        gas_tip_cap,
        gas_fee_cap,
        value,
        size,
        call_data_size,
        blob_gas,
        blob_gas_fee_cap,
        blob_hashes,
        is_cancel_shape
    FROM {{ index .dep "{{transformation}}" "int_transaction_mempool_observation_hourly" "helpers" "from" }} FINAL
    WHERE hour_start_date_time BETWEEN toStartOfHour(fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 7 DAY)
        AND toStartOfHour(fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 7 DAY)
),
-- earliest sighting per hash within the anchor window
anchor_first AS (
    SELECT
        hash,
        min(first_seen_date_time) AS first_seen
    FROM obs
    WHERE first_seen_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY hash
),
-- keep only hashes with no sighting in the 7 days before their anchor sighting
episodes AS (
    SELECT
        a.hash AS hash,
        a.first_seen AS first_seen
    FROM anchor_first a
    GLOBAL LEFT JOIN obs o ON o.hash = a.hash
    GROUP BY a.hash, a.first_seen
    HAVING countIf(o.first_seen_date_time < a.first_seen
        AND o.first_seen_date_time >= a.first_seen - INTERVAL 7 DAY) = 0
),
-- summarise each episode over its 7 day horizon
horizon AS (
    SELECT
        e.hash AS hash,
        e.first_seen AS first_seen,
        max(o.last_seen_date_time) AS last_seen,
        toUInt32(sum(o.sighting_count)) AS sighting_count,
        max(o.unique_sentries) AS peak_hourly_unique_sentries,
        argMin(o.`from`, o.first_seen_date_time) AS `from`,
        argMin(o.`to`, o.first_seen_date_time) AS `to`,
        argMin(o.nonce, o.first_seen_date_time) AS nonce,
        argMin(o.type, o.first_seen_date_time) AS type,
        argMin(o.gas, o.first_seen_date_time) AS gas,
        argMin(o.gas_price, o.first_seen_date_time) AS gas_price,
        argMin(o.gas_tip_cap, o.first_seen_date_time) AS gas_tip_cap,
        argMin(o.gas_fee_cap, o.first_seen_date_time) AS gas_fee_cap,
        argMin(o.value, o.first_seen_date_time) AS value,
        argMin(o.size, o.first_seen_date_time) AS size,
        argMin(o.call_data_size, o.first_seen_date_time) AS call_data_size,
        argMin(o.blob_gas, o.first_seen_date_time) AS blob_gas,
        argMin(o.blob_gas_fee_cap, o.first_seen_date_time) AS blob_gas_fee_cap,
        argMin(o.blob_hashes, o.first_seen_date_time) AS blob_hashes,
        argMin(o.is_cancel_shape, o.first_seen_date_time) AS is_cancel_shape,
        max(if(o.last_seen_date_time >= e.first_seen + INTERVAL 167 HOUR, 1, 0)) AS in_deadline_hour
    FROM episodes e
    GLOBAL INNER JOIN obs o ON o.hash = e.hash
    WHERE o.first_seen_date_time >= e.first_seen
        AND o.first_seen_date_time <= e.first_seen + INTERVAL 7 DAY
    GROUP BY e.hash, e.first_seen
),
-- inclusions of the observed nonce groups, wide enough to catch consumption before sighting
incl AS (
    SELECT
        hash,
        `from`,
        nonce,
        slot,
        slot_start_date_time,
        block_root,
        position
    FROM {{ index .dep "{{transformation}}" "fct_transaction_inclusion" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 14 DAY
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 7 DAY
        AND (`from`, nonce) GLOBAL IN (SELECT `from`, nonce FROM horizon)
),
-- this hash included within its horizon
own AS (
    SELECT
        h.hash AS hash,
        min(i.slot_start_date_time) AS islot_time,
        argMin(i.slot, i.slot_start_date_time) AS islot,
        argMin(i.block_root, i.slot_start_date_time) AS iroot,
        argMin(i.position, i.slot_start_date_time) AS ipos
    FROM horizon h
    GLOBAL INNER JOIN incl i ON i.hash = h.hash
    WHERE i.slot_start_date_time <= h.first_seen + INTERVAL 7 DAY
    GROUP BY h.hash
),
-- earliest inclusion of the nonce group within the horizon, any hash
winner AS (
    SELECT
        h.hash AS hash,
        argMin(i.hash, i.slot_start_date_time) AS whash,
        min(i.slot_start_date_time) AS wslot_time
    FROM horizon h
    GLOBAL INNER JOIN incl i ON i.`from` = h.`from` AND i.nonce = h.nonce
    WHERE i.slot_start_date_time <= h.first_seen + INTERVAL 7 DAY
    GROUP BY h.hash
),
-- relay evidence for including blocks
mev AS (
    SELECT DISTINCT
        slot_start_date_time,
        block_root
    FROM {{ index .dep "{{transformation}}" "fct_block_mev" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 14 DAY
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 7 DAY
        AND status = 'canonical'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    h.first_seen AS first_seen_date_time,
    h.hash AS hash,
    h.`from` AS `from`,
    h.`to` AS `to`,
    h.nonce AS nonce,
    h.type AS type,
    h.gas AS gas,
    h.gas_price AS gas_price,
    h.gas_tip_cap AS gas_tip_cap,
    h.gas_fee_cap AS gas_fee_cap,
    h.value AS value,
    h.size AS size,
    h.call_data_size AS call_data_size,
    h.blob_gas AS blob_gas,
    h.blob_gas_fee_cap AS blob_gas_fee_cap,
    h.blob_hashes AS blob_hashes,
    h.is_cancel_shape AS is_cancel_shape,
    h.last_seen AS last_seen_date_time,
    h.sighting_count AS sighting_count,
    h.peak_hourly_unique_sentries AS peak_hourly_unique_sentries,
    multiIf(
        o.islot_time IS NOT NULL, 'included',
        w.wslot_time IS NOT NULL, 'nonce_consumed',
        'unincluded'
    ) AS outcome,
    multiIf(
        o.islot_time IS NOT NULL, toDateTime64(o.islot_time, 3),
        w.wslot_time IS NOT NULL, toDateTime64(w.wslot_time, 3),
        h.first_seen + INTERVAL 7 DAY
    ) AS resolution_date_time,
    o.islot AS included_slot,
    o.islot_time AS included_slot_start_date_time,
    o.iroot AS included_block_root,
    o.ipos AS included_position,
    if(o.islot_time IS NOT NULL,
        dateDiff('millisecond', h.first_seen, toDateTime64(o.islot_time, 3)),
        NULL
    ) AS wait_ms,
    if(o.islot_time IS NOT NULL, m.block_root IS NOT NULL, NULL) AS included_via_known_relay,
    if(outcome = 'nonce_consumed', w.whash, NULL) AS winner_hash,
    if(outcome = 'nonce_consumed', w.wslot_time, NULL) AS winner_slot_start_date_time,
    ifNull(outcome = 'nonce_consumed' AND w.wslot_time < h.first_seen, 0) AS observed_after_nonce_consumed,
    if(outcome = 'unincluded', h.in_deadline_hour, 0) AS in_mempool_at_deadline
FROM horizon h
GLOBAL LEFT JOIN own o ON o.hash = h.hash
GLOBAL LEFT JOIN winner w ON w.hash = h.hash
GLOBAL LEFT JOIN mev m ON m.slot_start_date_time = o.islot_time AND m.block_root = o.iroot
SETTINGS join_use_nulls = 1
