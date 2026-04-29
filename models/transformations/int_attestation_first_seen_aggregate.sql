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
  - "{{external}}.libp2p_gossipsub_aggregate_and_proof"
  - "{{external}}.canonical_beacon_committee"
  - "{{external}}.canonical_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Dedup aggregates by (slot, committee_index, aggregation_bits) first. Many sentries see
-- the same aggregate — decoding every copy would be 50-100x slower.
-- Source is libp2p_gossipsub_aggregate_and_proof only. The beacon_api aggregate stream
-- mixes per-committee aggregates with EIP-7549 (post-Electra) multi-committee aggregates
-- whose bit layout cannot be decoded without committee_bits, which xatu does not expose.
WITH aggregates AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        committee_index,
        aggregation_bits,
        MIN(propagation_slot_start_diff) AS agg_seen,
        argMin(beacon_block_root, propagation_slot_start_diff) AS beacon_block_root,
        argMin(source_epoch, propagation_slot_start_diff) AS source_epoch,
        argMin(source_root, propagation_slot_start_diff) AS source_root,
        argMin(target_epoch, propagation_slot_start_diff) AS target_epoch,
        argMin(target_root, propagation_slot_start_diff) AS target_root
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_aggregate_and_proof" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
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

-- Restrict aggregates to those whose beacon_block_root is a canonical block within ~1 epoch
-- of the attestation slot. Without this filter, aggregates pointing at very-stale or
-- non-canonical block roots get bit-decoded against canonical's committee and produce
-- false validator attributions (xatu doesn't expose committee_bits, so post-Electra
-- multi-committee aggregates can't be reliably decoded). 32 slots is generous for
-- legitimate stalled-BN votes; older block roots are essentially always misattributions.
canonical_blocks AS (
    SELECT
        block_root,
        slot AS block_slot
    FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }} - 384) AND fromUnixTimestamp({{ .bounds.end }})
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
        a.beacon_block_root,
        a.source_epoch,
        a.source_root,
        a.target_epoch,
        a.target_root
    FROM aggregates a
    INNER JOIN committees c
        ON a.slot = c.slot AND a.committee_index = c.committee_index
    INNER JOIN canonical_blocks cb
        ON a.beacon_block_root = cb.block_root AND cb.block_slot >= a.slot - 32
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
    any(slot) AS slot,
    slot_start_date_time,
    any(epoch) AS epoch,
    any(epoch_start_date_time) AS epoch_start_date_time,
    attesting_validator_index,
    any(committee_index) AS committee_index,
    beacon_block_root AS block_root,
    source_epoch,
    source_root,
    target_epoch,
    target_root,
    MIN(agg_seen) AS seen_slot_start_diff,
    'libp2p_gossipsub_aggregate_and_proof' AS source
FROM exploded
GROUP BY slot_start_date_time, attesting_validator_index,
         beacon_block_root, source_epoch, source_root, target_epoch, target_root
