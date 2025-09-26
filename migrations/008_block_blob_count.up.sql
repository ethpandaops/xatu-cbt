CREATE TABLE `${NETWORK_NAME}`.fct_block_blob_count_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'The number of blobs in the block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Blob count of a block for the unfinalized chain. Forks in the chain may cause multiple block roots for the same slot to be present';

CREATE TABLE `${NETWORK_NAME}`.fct_block_blob_count_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_block_blob_count_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_blob_count_head_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_block_blob_count_head_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);

CREATE TABLE `${NETWORK_NAME}`.int_block_blob_count_canonical_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'The number of blobs in the block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`) COMMENT 'Blob count of a block for the finalized chain';

CREATE TABLE `${NETWORK_NAME}`.int_block_blob_count_canonical ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_block_blob_count_canonical_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_block_blob_count_canonical_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_block_blob_count_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'The number of blobs in the block' CODEC(DoubleDelta, ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Can be "canonical" or "orphaned"'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Blob count of a block for the finalized chain';

CREATE TABLE `${NETWORK_NAME}`.fct_block_blob_count ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_block_blob_count_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_blob_count_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_block_blob_count_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);
