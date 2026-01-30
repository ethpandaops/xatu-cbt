---
table: fct_attestation_vote_correctness_by_validator
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
  - canonical
  - vote_correctness
dependencies:
  - "{{transformation}}.int_attestation_attested_canonical"
  - "{{transformation}}.fct_block_proposer"
  - "{{external}}.canonical_beacon_committee"
  - "{{external}}.beacon_api_eth_v1_events_finalized_checkpoint"
  - "{{external}}.canonical_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Attestation data with source/target roots
WITH attestations AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
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

-- Committee duties for the bounds
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

-- Block data for head correctness checking
blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        `status`
    FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL '768 SECOND'
        AND slot_start_date_time <= fromUnixTimestamp({{ .bounds.end }})
),

-- Canonical target blocks: the block at the first slot of each epoch (or most recent before it)
-- The target root should point to the checkpoint block at the start of the target epoch
target_checkpoints AS (
    SELECT
        epoch AS target_epoch,
        argMax(block_root, slot) AS canonical_target_root
    FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL '1 DAY'
        AND slot_start_date_time <= fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY epoch
),

-- Finalized checkpoints for source correctness
-- The source root should match the finalized checkpoint at the source epoch
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
    duties.epoch AS epoch,
    duties.epoch_start_date_time AS epoch_start_date_time,
    duties.attesting_validator_index AS attesting_validator_index,
    -- Head correctness: slot_distance = 0 means correct head vote
    CASE
        WHEN attestations.head_root IS NULL THEN NULL
        WHEN blocks.slot IS NOT NULL AND duties.slot = blocks.slot THEN true
        WHEN blocks.slot IS NOT NULL AND duties.slot != blocks.slot THEN false
        ELSE NULL
    END AS head_correct,
    -- Target correctness: target_root matches the canonical target checkpoint
    CASE
        WHEN attestations.target_root IS NULL THEN NULL
        WHEN target_checkpoints.canonical_target_root IS NULL THEN NULL
        WHEN attestations.target_root = target_checkpoints.canonical_target_root THEN true
        ELSE false
    END AS target_correct,
    -- Source correctness: source_root matches the finalized checkpoint
    CASE
        WHEN attestations.source_root IS NULL THEN NULL
        WHEN finalized_checkpoints.finalized_root IS NULL THEN NULL
        WHEN attestations.source_root = finalized_checkpoints.finalized_root THEN true
        ELSE false
    END AS source_correct,
    attestations.inclusion_distance AS inclusion_distance,
    CASE
        WHEN attestations.head_root IS NULL THEN 'missed'
        WHEN blocks.status IS NOT NULL THEN blocks.status
        ELSE 'unknown'
    END AS status
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

