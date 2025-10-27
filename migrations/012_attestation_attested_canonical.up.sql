CREATE TABLE `${NETWORK_NAME}`.int_attestation_attested_canonical_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `source_epoch` UInt32 COMMENT 'The source epoch number in the attestation group',
    `source_epoch_start_date_time` DateTime COMMENT 'The wall clock time when the source epoch started',
    `source_root` FixedString(66) COMMENT 'The source beacon block root hash in the attestation group',
    `target_epoch` UInt32 COMMENT 'The target epoch number in the attestation group',
    `target_epoch_start_date_time` DateTime COMMENT 'The wall clock time when the target epoch started',
    `target_root` FixedString(66) COMMENT 'The target beacon block root hash in the attestation group',
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `attesting_validator_index` UInt32 COMMENT 'The index of the validator attesting' CODEC(ZSTD(1)),
    `inclusion_distance` UInt32 COMMENT 'The distance from the slot when the attestation was included' CODEC(ZSTD(1)),
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`, `attesting_validator_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Attested head of a block for the unfinalized chain.';

CREATE TABLE `${NETWORK_NAME}`.int_attestation_attested_canonical ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_attestation_attested_canonical_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_attestation_attested_canonical_local,
    cityHash64(`slot_start_date_time`, `block_root`, `attesting_validator_index`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_canonical_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `votes_max` UInt32 COMMENT 'The maximum number of scheduled votes for the block' CODEC(ZSTD(1)),
    `votes_head` Nullable(UInt32) COMMENT 'The number of votes for the block proposed in the current slot' CODEC(ZSTD(1)),
    `votes_other` Nullable(UInt32) COMMENT 'The number of votes for any blocks proposed in previous slots' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Attestation correctness of a block for the finalized chain';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_canonical ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_correctness_canonical_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_correctness_canonical_local,
    cityHash64(`slot_start_date_time`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_attestation_correctness_canonical_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_by_validator_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `attesting_validator_index` UInt32 COMMENT 'The index of the validator attesting' CODEC(ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root hash that was attested, null means the attestation was missed' CODEC(ZSTD(1)),
    `slot_distance` Nullable(UInt32) COMMENT 'The distance from the slot to the attested block. If the attested block is the same as the slot, the distance is 0, if the attested block is the previous slot, the distance is 1, etc. If null, the attestation was missed, the block was orphaned and never seen by a sentry or the block was more than 64 slots ago' CODEC(DoubleDelta, ZSTD(1)),
    `propagation_distance` Nullable(UInt32) COMMENT 'The distance from the slot when the attestation was propagated. 0 means the attestation was propagated within the same slot as its duty was assigned, 1 means the attestation was propagated within the next slot, etc.' CODEC(DoubleDelta, ZSTD(1)),
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `attesting_validator_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Attestation correctness by validator for the finalized chain';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_by_validator_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_correctness_by_validator_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_correctness_by_validator_head_local,
    cityHash64(`slot_start_date_time`, `attesting_validator_index`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_by_validator_canonical_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `attesting_validator_index` UInt32 COMMENT 'The index of the validator attesting' CODEC(ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root hash that was attested' CODEC(ZSTD(1)),
    `slot_distance` Nullable(UInt32) COMMENT 'The distance from the slot to the attested block. If the attested block is the same as the slot, the distance is 0, if the attested block is the previous slot, the distance is 1, etc. If null, the attestation was missed, the block was orphaned and never seen by a sentry or the block was more than 64 slots ago' CODEC(DoubleDelta, ZSTD(1)),
    `inclusion_distance` Nullable(UInt32) COMMENT 'The distance from the slot when the attestation was included in a block' CODEC(DoubleDelta, ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Can be "canonical", "orphaned", "missed" or "unknown" (validator attested but block data not available)',
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `attesting_validator_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Attestation correctness by validator for the finalized chain';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_by_validator_canonical ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_correctness_by_validator_canonical_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_correctness_by_validator_canonical_local,
    cityHash64(`slot_start_date_time`, `attesting_validator_index`)
);
