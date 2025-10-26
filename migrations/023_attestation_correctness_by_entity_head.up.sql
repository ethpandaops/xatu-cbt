CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_by_entity_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `entity` Nullable(String) COMMENT 'The entity (staking provider) associated with the validators' CODEC(ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root hash that was attested, null means the attestation was missed' CODEC(ZSTD(1)),
    `attestation_count` UInt32 COMMENT 'Number of attestations for this entity/block_root combination' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot`, ifNull(`block_root`, ''), ifNull(`entity`, ''))
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Attestation correctness aggregated by entity for the head chain. Multiple rows per slot when entities attest to different block roots (forks)';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_correctness_by_entity_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_correctness_by_entity_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_correctness_by_entity_head_local,
    cityHash64(`slot`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_attestation_correctness_by_entity_head_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot_start_date_time
(
    SELECT *
    ORDER BY (`slot_start_date_time`)
);
