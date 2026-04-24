---
table: int_attestation_first_seen_aggregate
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - attestation
  - aggregate
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_attestation"
  - "{{external}}.libp2p_gossipsub_aggregate_and_proof"
  - "{{external}}.canonical_beacon_committee"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Dedup aggregates by (slot, committee_index, aggregation_bits) first. Many sentries see
-- the same aggregate — decoding every copy would be 50-100x slower.
WITH aggregates AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        committee_index,
        aggregation_bits,
        MIN(propagation_slot_start_diff) AS agg_seen,
        argMin(source, propagation_slot_start_diff) AS source,
        argMin(beacon_block_root, propagation_slot_start_diff) AS beacon_block_root,
        argMin(source_epoch, propagation_slot_start_diff) AS source_epoch,
        argMin(source_root, propagation_slot_start_diff) AS source_root,
        argMin(target_epoch, propagation_slot_start_diff) AS target_epoch,
        argMin(target_root, propagation_slot_start_diff) AS target_root
    FROM (
        SELECT
            'beacon_api_eth_v1_events_attestation' AS source,
            slot, slot_start_date_time, epoch, epoch_start_date_time,
            committee_index, aggregation_bits, propagation_slot_start_diff,
            beacon_block_root, source_epoch, source_root, target_epoch, target_root
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_attestation" "helpers" "from" }}
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
            AND aggregation_bits != ''

        UNION ALL

        SELECT
            'libp2p_gossipsub_aggregate_and_proof' AS source,
            slot, slot_start_date_time, epoch, epoch_start_date_time,
            committee_index, aggregation_bits, propagation_slot_start_diff,
            beacon_block_root, source_epoch, source_root, target_epoch, target_root
        FROM {{ index .dep "{{external}}" "libp2p_gossipsub_aggregate_and_proof" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
    )
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time,
             committee_index, aggregation_bits
),

committees AS (
    SELECT
        slot,
        committee_index,
        validators,
        length(validators) AS committee_size
    FROM {{ index .dep "{{external}}" "canonical_beacon_committee" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

-- Explode each distinct aggregate into one row per attesting validator by decoding
-- the SSZ aggregation_bits bitlist. Bits are LSB-first within each byte and we only
-- iterate positions [0, committee_size) so the SSZ length-sentinel bit is never
-- interpreted as an attester.
exploded AS (
    SELECT
        a.slot,
        a.slot_start_date_time,
        a.epoch,
        a.epoch_start_date_time,
        a.committee_index,
        c.validators[p + 1] AS attesting_validator_index,
        a.agg_seen,
        a.source,
        a.beacon_block_root,
        a.source_epoch,
        a.source_root,
        a.target_epoch,
        a.target_root
    FROM aggregates a
    INNER JOIN committees c
        ON a.slot = c.slot AND a.committee_index = c.committee_index
    ARRAY JOIN arrayFilter(
        pos -> bitTest(
            reinterpretAsUInt8(substring(unhex(substring(a.aggregation_bits, 3)), intDiv(pos, 8) + 1, 1)),
            pos % 8
        ) = 1,
        arrayMap(i -> i - 1, range(1, c.committee_size + 1))
    ) AS p
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, agg_seen) AS slot,
    slot_start_date_time,
    argMin(epoch, agg_seen) AS epoch,
    argMin(epoch_start_date_time, agg_seen) AS epoch_start_date_time,
    attesting_validator_index,
    argMin(committee_index, agg_seen) AS committee_index,
    MIN(agg_seen) AS seen_slot_start_diff,
    argMin(source, agg_seen) AS source,
    argMin(beacon_block_root, agg_seen) AS block_root,
    argMin(source_epoch, agg_seen) AS source_epoch,
    argMin(source_root, agg_seen) AS source_root,
    argMin(target_epoch, agg_seen) AS target_epoch,
    argMin(target_root, agg_seen) AS target_root
FROM exploded
GROUP BY slot_start_date_time, attesting_validator_index
