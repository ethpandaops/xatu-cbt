CREATE TABLE `${NETWORK_NAME}`.fct_mev_bid_count_by_builder_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number within the block bid' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time for the slot that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The start time for the epoch that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `builder_pubkey` String COMMENT 'The relay that the bid was fetched from' CODEC(ZSTD(1)),
    `bid_total` UInt32 COMMENT 'The total number of bids from the builder' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `builder_pubkey`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 4,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Total number of MEV builder bids for a slot';

CREATE TABLE `${NETWORK_NAME}`.fct_mev_bid_count_by_builder ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_mev_bid_count_by_builder_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_mev_bid_count_by_builder_local,
    cityHash64(`slot_start_date_time`, `builder_pubkey`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_mev_bid_count_by_builder_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `builder_pubkey`)
);
