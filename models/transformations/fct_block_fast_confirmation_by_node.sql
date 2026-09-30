---
table: fct_block_fast_confirmation_by_node
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - fast_confirmation
dependencies:
  - "{{external}}.canonical_beacon_block"
  - "{{external}}.beacon_api_eth_v1_events_fast_confirmation"
  - "{{external}}.beacon_api_eth_v1_events_finalized_checkpoint"
---
-- A fast confirmation event confirms its block and every ancestor, so a canonical block
-- without its own event counts as confirmed at the first later event from the same node.
-- A node is only scored for a block when it emitted a confirmation in the 64 slots before
-- the block's slot ended, which keeps node start up and long outages out of the scores.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
    blocks AS (
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            epoch_start_date_time,
            block_root
        FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
    ),
    confirmations AS (
        SELECT
            meta_client_name,
            slot,
            block,
            slot_start_date_time,
            epoch,
            epoch_start_date_time,
            min(event_date_time) AS confirmed_date_time,
            argMin(meta_consensus_implementation, event_date_time) AS meta_consensus_implementation,
            argMin(meta_consensus_version, event_date_time) AS meta_consensus_version
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_fast_confirmation" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 30 MINUTE
                AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 30 MINUTE
            AND meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY meta_client_name, slot, block, slot_start_date_time, epoch, epoch_start_date_time
    ),
    nodes AS (
        SELECT
            meta_client_name,
            argMax(meta_consensus_implementation, confirmed_date_time) AS meta_consensus_implementation,
            argMax(meta_consensus_version, confirmed_date_time) AS meta_consensus_version
        FROM confirmations
        GROUP BY meta_client_name
    ),
    finalizations AS (
        SELECT
            meta_network_name,
            epoch,
            min(event_date_time) AS finalized_date_time
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_finalized_checkpoint" "helpers" "from" }} FINAL
        WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR
                AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 2 HOUR
            AND meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY meta_network_name, epoch
    ),
    grid AS (
        SELECT
            b.slot AS slot,
            b.slot_start_date_time AS slot_start_date_time,
            b.epoch AS epoch,
            b.epoch_start_date_time AS epoch_start_date_time,
            b.block_root AS block_root,
            '{{ .env.NETWORK }}' AS meta_network_name,
            -- Checkpoint epoch E finalizes every block up to slot E * 32.
            toUInt32(intDiv(b.slot + 31, 32)) AS finality_epoch,
            toDateTime64(b.slot_start_date_time, 3) + INTERVAL 12 SECOND AS slot_end_date_time,
            n.meta_client_name AS meta_client_name,
            n.meta_consensus_implementation AS meta_consensus_implementation,
            n.meta_consensus_version AS meta_consensus_version
        FROM blocks AS b
        CROSS JOIN nodes AS n
    ),
    with_direct AS (
        SELECT
            g.*,
            d.confirmed_date_time AS direct_date_time
        FROM grid AS g
        GLOBAL LEFT JOIN confirmations AS d
            ON d.meta_client_name = g.meta_client_name
            AND d.slot = g.slot
            AND d.block = g.block_root
    ),
    with_liveness AS (
        SELECT
            w.*,
            p.confirmed_date_time AS last_emitted_date_time
        FROM with_direct AS w
        GLOBAL ASOF LEFT JOIN confirmations AS p
            ON p.meta_client_name = w.meta_client_name
            AND p.confirmed_date_time <= w.slot_end_date_time
    ),
    with_descendant AS (
        SELECT
            w.*,
            a.confirmed_date_time AS descendant_date_time,
            a.slot AS descendant_slot
        FROM with_liveness AS w
        GLOBAL ASOF LEFT JOIN confirmations AS a
            ON a.meta_client_name = w.meta_client_name
            AND a.slot > w.slot
        WHERE w.last_emitted_date_time IS NOT NULL
            AND w.last_emitted_date_time >= w.slot_end_date_time - INTERVAL 768 SECOND
    ),
    canonical AS (
        SELECT
            w.slot AS slot,
            w.slot_start_date_time AS slot_start_date_time,
            w.epoch AS epoch,
            w.epoch_start_date_time AS epoch_start_date_time,
            w.block_root AS block_root,
            w.meta_client_name AS meta_client_name,
            w.meta_consensus_implementation AS meta_consensus_implementation,
            w.meta_consensus_version AS meta_consensus_version,
            -- A block's own event can arrive after a later block already confirmed it, so the
            -- earlier of the two counts.
            w.descendant_date_time IS NOT NULL AND w.descendant_slot - w.slot <= 150 AS has_descendant,
            multiIf(
                w.direct_date_time IS NOT NULL
                    AND (NOT has_descendant OR w.direct_date_time <= w.descendant_date_time), 'direct',
                has_descendant, 'descendant',
                'unconfirmed'
            ) AS confirmation_type,
            multiIf(
                confirmation_type = 'direct', w.direct_date_time,
                confirmation_type = 'descendant', w.descendant_date_time,
                NULL
            ) AS fast_confirmed_date_time,
            multiIf(
                confirmation_type = 'direct', w.slot,
                confirmation_type = 'descendant', w.descendant_slot,
                NULL
            ) AS fast_confirmation_slot,
            f.finalized_date_time AS finalized_date_time,
            f.epoch AS finalized_epoch
        FROM with_descendant AS w
        GLOBAL ASOF LEFT JOIN finalizations AS f
            ON f.meta_network_name = w.meta_network_name
            AND f.epoch >= w.finality_epoch
    ),
    orphaned AS (
        SELECT
            c.slot AS slot,
            c.slot_start_date_time AS slot_start_date_time,
            coalesce(c.epoch, toUInt32(intDiv(c.slot, 32))) AS epoch,
            coalesce(c.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
            c.block AS block_root,
            c.meta_client_name AS meta_client_name,
            c.meta_consensus_implementation AS meta_consensus_implementation,
            c.meta_consensus_version AS meta_consensus_version,
            'direct' AS confirmation_type,
            c.confirmed_date_time AS fast_confirmed_date_time,
            c.slot AS fast_confirmation_slot
        FROM confirmations AS c
        GLOBAL LEFT ANTI JOIN blocks AS b
            ON b.block_root = c.block
        WHERE c.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    'canonical' AS status,
    meta_client_name,
    meta_consensus_implementation,
    meta_consensus_version,
    confirmation_type,
    toUInt32(toUnixTimestamp64Milli(fast_confirmed_date_time) - toUnixTimestamp(slot_start_date_time) * 1000) AS fast_confirmed_slot_start_diff,
    fast_confirmation_slot,
    toUInt32(toUnixTimestamp64Milli(finalized_date_time) - toUnixTimestamp(slot_start_date_time) * 1000) AS finalized_slot_start_diff,
    finalized_epoch
FROM canonical

UNION ALL

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    'orphaned' AS status,
    meta_client_name,
    meta_consensus_implementation,
    meta_consensus_version,
    confirmation_type,
    toUInt32(toUnixTimestamp64Milli(fast_confirmed_date_time) - toUnixTimestamp(slot_start_date_time) * 1000) AS fast_confirmed_slot_start_diff,
    fast_confirmation_slot,
    NULL AS finalized_slot_start_diff,
    NULL AS finalized_epoch
FROM orphaned
SETTINGS join_use_nulls = 1
