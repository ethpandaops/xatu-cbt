-- Create local table with ReplicatedReplacingMergeTree
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `date` Date COMMENT 'The calendar date' CODEC(DoubleDelta, ZSTD(1)),
    `entity` String COMMENT 'The entity (staking provider) associated with the validators, unknown if not mapped' CODEC(ZSTD(1)),
    `status` String COMMENT 'Attestation status: attested or missed' CODEC(ZSTD(1)),
    `attestation_count` UInt64 COMMENT 'Sum of attestations for this date/entity/status combination' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(date)
ORDER BY
    (`date`, `entity`, `status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Attestation liveness aggregated by entity and day for the head chain. Reduces epoch-level data by ~225x.';

-- Create distributed table wrapper
CREATE TABLE `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head_daily ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_liveness_by_entity_head_daily_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_liveness_by_entity_head_daily_local,
    cityHash64(`date`)
);
