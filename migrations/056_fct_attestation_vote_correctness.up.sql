CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Time dimensions
    `slot` UInt32 COMMENT 'The slot number of the attestation duty' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),

    -- Validator
    `attesting_validator_index` UInt32 COMMENT 'The index of the validator with the attestation duty' CODEC(ZSTD(1)),

    -- Vote correctness
    `head_correct` Nullable(Bool) COMMENT 'Whether the head vote was correct (voted for block at same slot). NULL if attestation missed or block data unavailable' CODEC(ZSTD(1)),
    `target_correct` Nullable(Bool) COMMENT 'Whether the target vote was correct (target_root matches canonical checkpoint). NULL if attestation missed or checkpoint data unavailable' CODEC(ZSTD(1)),
    `source_correct` Nullable(Bool) COMMENT 'Whether the source vote was correct (source_root matches finalized checkpoint). NULL if attestation missed or checkpoint data unavailable' CODEC(ZSTD(1)),

    -- Inclusion
    `inclusion_distance` Nullable(UInt32) COMMENT 'The distance from the slot when the attestation was included in a block. NULL if missed' CODEC(DoubleDelta, ZSTD(1)),

    -- Status
    `status` LowCardinality(String) COMMENT 'Attestation status: canonical, orphaned, missed, or unknown'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (slot_start_date_time, attesting_validator_index)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-validator attestation vote correctness tracking head, target, and source votes';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_vote_correctness_by_validator_local,
    cityHash64(slot_start_date_time, attesting_validator_index)
);

ALTER TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (slot, attesting_validator_index)
);

ALTER TABLE `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_validator
(
    SELECT *
    ORDER BY (attesting_validator_index, slot_start_date_time)
);
