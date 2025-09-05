CREATE TABLE `${NETWORK_NAME}`.int_blocks_orphaned_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot of the orphaned (reorged) block' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'Beacon block root hash (orphaned)' CODEC(ZSTD(1)),
    `proposer_index` Nullable(UInt32) COMMENT 'Proposer index' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`) COMMENT 'Blocks that were seen but are not part of the canonical chain up to the latest canonical epoch';

CREATE TABLE `${NETWORK_NAME}`.int_blocks_orphaned ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_blocks_orphaned_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_blocks_orphaned_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);
