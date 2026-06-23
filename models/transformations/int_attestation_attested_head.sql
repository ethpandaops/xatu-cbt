---
table: int_attestation_attested_head
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - attestation
  - head
dependencies:
  - - "{{external}}.beacon_api_eth_v1_events_attestation"
    - "{{external}}.libp2p_gossipsub_beacon_attestation"
    - "{{external}}.canonical_beacon_elaborated_attestation"
  - "{{transformation}}.int_beacon_committee_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Get the validator indices for the validators that were in the beacon committee for the slot
WITH validator_indices AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        arrayJoin(validators) AS validator_index
    FROM {{ index .dep "{{transformation}}" "int_beacon_committee_head" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

-- Get the events for the attestations that were captured. The two real-time
-- sources (beacon API event stream + libp2p gossip) only ever see the subset
-- of attesters whose attestations propagated to a sentry before the slot was
-- processed (~35-66% at the head). canonical_beacon_elaborated_attestation is
-- unioned in as a third source so that backfilled/finalized history is sourced
-- from the chain-included attestation set (~99% participation) instead. The
-- three sources form an OR-group dependency, so forwardfill is not capped at
-- canonical's lag: at the head canonical's range is empty and it contributes
-- nothing, leaving the real-time sources to serve the live tip, while in
-- backfilled history it provides the complete attester set. Canonical rows
-- carry no gossip-propagation timing, so their propagation_distance is NULL
-- (head rows keep their measured propagation; min() ignores the NULLs).
combined_events AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root,
        source_epoch,
        source_epoch_start_date_time,
        source_root,
        target_epoch,
        target_epoch_start_date_time,
        target_root,
        attesting_validator_index,
        floor(min(propagation_slot_start_diff) / 12000) AS propagation_distance
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_attestation" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND aggregation_bits = ''
        AND attesting_validator_index IS NOT NULL
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root, source_epoch, source_epoch_start_date_time, source_root, target_epoch, target_epoch_start_date_time, target_root, attesting_validator_index

    UNION ALL

    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root,
        source_epoch,
        source_epoch_start_date_time,
        source_root,
        target_epoch,
        target_epoch_start_date_time,
        target_root,
        attesting_validator_index,
        floor(min(propagation_slot_start_diff) / 12000) AS propagation_distance
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_beacon_attestation" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND aggregation_bits = ''
        AND attesting_validator_index IS NOT NULL
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root, source_epoch, source_epoch_start_date_time, source_root, target_epoch, target_epoch_start_date_time, target_root, attesting_validator_index

    UNION ALL

    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        beacon_block_root,
        source_epoch,
        source_epoch_start_date_time,
        source_root,
        target_epoch,
        target_epoch_start_date_time,
        target_root,
        attesting_validator_index,
        CAST(NULL AS Nullable(UInt32)) AS propagation_distance
    FROM (
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            epoch_start_date_time,
            beacon_block_root,
            source_epoch,
            source_epoch_start_date_time,
            source_root,
            target_epoch,
            target_epoch_start_date_time,
            target_root,
            arrayJoin(validators) AS attesting_validator_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_elaborated_attestation" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND meta_network_name = '{{ .env.NETWORK }}'
    )
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root, source_epoch, source_epoch_start_date_time, source_root, target_epoch, target_epoch_start_date_time, target_root, attesting_validator_index
),

-- Make sure we only have events for validators in the validator_indices table
filtered_events AS (
    SELECT
        ce.slot,
        ce.slot_start_date_time,
        ce.epoch,
        ce.epoch_start_date_time,
        ce.beacon_block_root,
        ce.source_epoch,
        ce.source_epoch_start_date_time,
        ce.source_root,
        ce.target_epoch,
        ce.target_epoch_start_date_time,
        ce.target_root,
        ce.attesting_validator_index,
        ce.propagation_distance
    FROM combined_events ce
    GLOBAL INNER JOIN validator_indices vi ON ce.slot = vi.slot AND ce.attesting_validator_index = vi.validator_index
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    source_epoch,
    source_epoch_start_date_time,
    source_root,
    target_epoch,
    target_epoch_start_date_time,
    target_root,
    beacon_block_root AS block_root,
    attesting_validator_index,
    min(propagation_distance) AS propagation_distance
FROM filtered_events
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, beacon_block_root, source_epoch, source_epoch_start_date_time, source_root, target_epoch, target_epoch_start_date_time, target_root, attesting_validator_index
