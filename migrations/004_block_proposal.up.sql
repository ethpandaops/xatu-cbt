CREATE TABLE `${NETWORK_NAME}`.fct_block_proposer_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `proposer_validator_index` UInt32 COMMENT 'The validator index of the proposer for the slot' CODEC(ZSTD(1)),
    `proposer_pubkey` String COMMENT 'The public key of the validator proposer' CODEC(ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root hash. Null if a block was never seen by a sentry' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `proposer_validator_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Block proposers for the unfinalized chain';

CREATE TABLE `${NETWORK_NAME}`.fct_block_proposer_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_block_proposer_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_proposer_head_local,
    cityHash64(`slot_start_date_time`, `proposer_validator_index`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_block_proposer_head_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `proposer_validator_index`)
);

CREATE TABLE `${NETWORK_NAME}`.int_block_proposer_canonical_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `proposer_validator_index` UInt32 COMMENT 'The validator index of the proposer for the slot' CODEC(ZSTD(1)),
    `proposer_pubkey` String COMMENT 'The public key of the validator proposer' CODEC(ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root hash. Null if a slot was missed' CODEC(ZSTD(1))
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
COMMENT 'Block proposers for the finalized chain';

CREATE TABLE `${NETWORK_NAME}`.int_block_proposer_canonical ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_block_proposer_canonical_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_block_proposer_canonical_local,
    cityHash64(`slot_start_date_time`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_block_proposer_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `proposer_validator_index` UInt32 COMMENT 'The validator index of the proposer for the slot' CODEC(ZSTD(1)),
    `proposer_pubkey` String COMMENT 'The public key of the validator proposer' CODEC(ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root hash. Null if a block was never seen by a sentry, aka "missed"' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Can be "canonical", "orphaned" or "missed"'
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
COMMENT 'Block proposers for the finalized chain including orphaned blocks';

CREATE TABLE `${NETWORK_NAME}`.fct_block_proposer ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_block_proposer_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_proposer_local,
    cityHash64(`slot_start_date_time`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_block_proposer_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`)
);
