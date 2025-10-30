-- Create local table with ReplicatedReplacingMergeTree
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_epoch_head_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `entity` String COMMENT 'The entity (staking provider) associated with the validators, unknown if not mapped' CODEC(ZSTD(1)),
    `status` String COMMENT 'Attestation status: attested or missed' CODEC(ZSTD(1)),
    `attestation_count` UInt64 COMMENT 'Sum of attestations for this epoch/entity/status combination' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(epoch_start_date_time)
ORDER BY
    (`epoch_start_date_time`, `entity`, `status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Attestation liveness aggregated by entity and epoch for the head chain. Reduces slot-level data by ~32x.';

-- Create distributed table wrapper
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_epoch_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_epoch_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_liveness_by_entity_epoch_head_local,
    cityHash64(`epoch`)
);

-- Add secondary projection for efficient epoch number queries
ALTER TABLE `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_epoch_head_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_epoch
(
    SELECT *
    ORDER BY (`epoch`)
);
