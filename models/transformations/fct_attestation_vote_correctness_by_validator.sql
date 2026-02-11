---
table: fct_attestation_vote_correctness_by_validator
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - slot
  - attestation
  - canonical
  - vote_correctness
  - validator_performance
dependencies:
  - "{{transformation}}.int_attestation_attested_canonical"
  - "{{transformation}}.fct_block_proposer"
  - "{{external}}.canonical_beacon_committee"
  - "{{external}}.beacon_api_eth_v1_events_finalized_checkpoint"
  - "{{external}}.canonical_beacon_block"
---
-- Per-slot attestation vote correctness by validator. One row per validator per slot
-- where the validator had an attestation duty.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Committee duties for slots within bounds
    duties AS (
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            epoch_start_date_time,
            arrayJoin(validators) AS attesting_validator_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_committee" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),

    -- Attestation data with source/target roots for slots in bounds
    attestations AS (
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            block_root AS head_root,
            source_epoch,
            source_root,
            target_epoch,
            target_root,
            attesting_validator_index,
            inclusion_distance
        FROM {{ index .dep "{{transformation}}" "int_attestation_attested_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),

    -- Block data for head correctness checking (with extra buffer for lookback)
    blocks AS (
        SELECT
            slot,
            slot_start_date_time,
            epoch,
            block_root,
            `status`
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL '768 SECOND'
          AND slot_start_date_time <= fromUnixTimestamp({{ .bounds.end }})
    ),

    -- Canonical target blocks: the block at the epoch boundary (first slot of epoch),
    -- or if that slot is missed, the last block from the previous epoch
    epoch_blocks AS (
        SELECT
            epoch,
            min(slot) AS first_block_slot,
            argMin(block_root, slot) AS first_block_root,
            argMax(block_root, slot) AS last_block_root
        FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL '1 DAY'
          AND slot_start_date_time <= fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY epoch
    ),
    target_checkpoints AS (
        SELECT
            curr.epoch AS target_epoch,
            -- If first block is at epoch boundary (slot = epoch * 32), use it
            -- Otherwise, use the last block from the previous epoch
            if(curr.first_block_slot = curr.epoch * 32,
               curr.first_block_root,
               prev.last_block_root) AS canonical_target_root
        FROM epoch_blocks curr
        LEFT JOIN epoch_blocks prev ON prev.epoch = curr.epoch - 1
    ),

    -- Finalized checkpoints for source correctness
    finalized_checkpoints AS (
        SELECT
            epoch AS source_epoch,
            argMax(block, event_date_time) AS finalized_root
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_finalized_checkpoint" "helpers" "from" }} FINAL
        WHERE epoch_start_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL '1 DAY'
          AND epoch_start_date_time <= fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY epoch
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    duties.slot AS slot,
    duties.slot_start_date_time AS slot_start_date_time,
    duties.attesting_validator_index AS validator_index,
    -- Whether attested (not missed)
    attestations.head_root IS NOT NULL AS attested,
    -- Head correctness: slot_distance = 0 means correct head vote
    CASE
        WHEN attestations.head_root IS NULL THEN NULL
        WHEN blocks.slot IS NOT NULL AND duties.slot = blocks.slot THEN true
        WHEN blocks.slot IS NOT NULL AND duties.slot != blocks.slot THEN false
        ELSE NULL
    END AS head_correct,
    -- Target correctness
    CASE
        WHEN attestations.target_root IS NULL THEN NULL
        WHEN target_checkpoints.canonical_target_root IS NULL THEN NULL
        WHEN attestations.target_root = target_checkpoints.canonical_target_root THEN true
        ELSE false
    END AS target_correct,
    -- Source correctness
    CASE
        WHEN attestations.source_root IS NULL THEN NULL
        WHEN finalized_checkpoints.finalized_root IS NULL THEN NULL
        WHEN attestations.source_root = finalized_checkpoints.finalized_root THEN true
        ELSE false
    END AS source_correct,
    attestations.inclusion_distance AS inclusion_distance
FROM duties
LEFT JOIN attestations ON
    duties.slot = attestations.slot
    AND duties.attesting_validator_index = attestations.attesting_validator_index
LEFT JOIN blocks ON
    attestations.head_root = blocks.block_root
    AND attestations.head_root IS NOT NULL
LEFT JOIN target_checkpoints ON
    attestations.target_epoch = target_checkpoints.target_epoch
    AND attestations.target_root IS NOT NULL
LEFT JOIN finalized_checkpoints ON
    attestations.source_epoch = finalized_checkpoints.source_epoch
    AND attestations.source_root IS NOT NULL
SETTINGS join_use_nulls = 1
