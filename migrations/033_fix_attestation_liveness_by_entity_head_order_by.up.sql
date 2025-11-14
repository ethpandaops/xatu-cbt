-- Fix ORDER BY clause to prevent duplicate rows when validator status changes from missed to attested
-- Issue: When attestations arrive late, validators transition from NULL block_root (missed) to non-NULL (attested)
-- The old ORDER BY (slot_start_date_time, entity, status) treats these as separate rows
-- This fix removes 'status' from ORDER BY so ReplacingMergeTree properly replaces old records

-- Drop existing tables
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head_local ON CLUSTER '{cluster}';

-- Recreate local table with corrected ORDER BY
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `entity` String COMMENT 'The entity (staking provider) associated with the validators, unknown if not mapped' CODEC(ZSTD(1)),
    `status` String COMMENT 'Attestation status: attested or missed' CODEC(ZSTD(1)),
    `attestation_count` UInt32 COMMENT 'Number of attestations for this entity/status combination' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `entity`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Attestation liveness aggregated by entity for the head chain. Multiple rows per (slot, entity) for different statuses (attested/missed).';

-- Recreate distributed table
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_liveness_by_entity_head_local,
    cityHash64(`slot`)
);

-- Recreate projection
ALTER TABLE `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`)
);
