CREATE TABLE `${NETWORK_NAME}`.fct_block_mev_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number within the block proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time for the slot that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The start time for the epoch that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The root hash of the beacon block' CODEC(ZSTD(1)),
    `earliest_bid_date_time` Nullable(DateTime64(3)) COMMENT 'The earliest timestamp of the accepted bid in milliseconds' CODEC(ZSTD(1)),
    `relay_names` Array(String) COMMENT 'The relay names that delivered the proposer payload' CODEC(ZSTD(1)),
    `parent_hash` FixedString(66) COMMENT 'The parent hash of the proposer payload' CODEC(ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'The block hash of the proposer payload' CODEC(ZSTD(1)),
    `builder_pubkey` String COMMENT 'The builder pubkey of the proposer payload' CODEC(ZSTD(1)),
    `proposer_pubkey` String COMMENT 'The proposer pubkey of the proposer payload' CODEC(ZSTD(1)),
    `proposer_fee_recipient` FixedString(42) COMMENT 'The proposer fee recipient of the proposer payload' CODEC(ZSTD(1)),
    `gas_limit` UInt64 COMMENT 'The gas limit of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `gas_used` UInt64 COMMENT 'The gas used of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `value` Nullable(UInt128) COMMENT 'The transaction value in wei' CODEC(ZSTD(1)),
    `transaction_count` UInt32 COMMENT 'The number of transactions in the proposer payload' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'MEV relay proposer payload delivered for a block on the unfinalized chain';

CREATE TABLE `${NETWORK_NAME}`.fct_block_mev_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_block_mev_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_mev_head_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_block_mev_head_local
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);

CREATE TABLE `${NETWORK_NAME}`.int_block_mev_canonical_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number within the block proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time for the slot that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The start time for the epoch that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The root hash of the beacon block' CODEC(ZSTD(1)),
    `earliest_bid_date_time` Nullable(DateTime64(3)) COMMENT 'The earliest timestamp of the accepted bid in milliseconds' CODEC(ZSTD(1)),
    `relay_names` Array(String) COMMENT 'The relay names that delivered the proposer payload' CODEC(ZSTD(1)),
    `parent_hash` FixedString(66) COMMENT 'The parent hash of the proposer payload' CODEC(ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'The block hash of the proposer payload' CODEC(ZSTD(1)),
    `builder_pubkey` String COMMENT 'The builder pubkey of the proposer payload' CODEC(ZSTD(1)),
    `proposer_pubkey` String COMMENT 'The proposer pubkey of the proposer payload' CODEC(ZSTD(1)),
    `proposer_fee_recipient` FixedString(42) COMMENT 'The proposer fee recipient of the proposer payload' CODEC(ZSTD(1)),
    `gas_limit` UInt64 COMMENT 'The gas limit of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `gas_used` UInt64 COMMENT 'The gas used of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `value` Nullable(UInt128) COMMENT 'The transaction value in wei' CODEC(ZSTD(1)),
    `transaction_count` UInt32 COMMENT 'The number of transactions in the proposer payload' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'MEV relay proposer payload delivered for a block on the finalized chain';

CREATE TABLE `${NETWORK_NAME}`.int_block_mev_canonical ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_block_mev_canonical_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_block_mev_canonical_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_block_mev_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number within the block proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time for the slot that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The start time for the epoch that the proposer payload is for' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The root hash of the beacon block' CODEC(ZSTD(1)),
    `earliest_bid_date_time` Nullable(DateTime64(3)) COMMENT 'The earliest timestamp of the accepted bid in milliseconds' CODEC(ZSTD(1)),
    `relay_names` Array(String) COMMENT 'The relay names that delivered the proposer payload' CODEC(ZSTD(1)),
    `parent_hash` FixedString(66) COMMENT 'The parent hash of the proposer payload' CODEC(ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'The block hash of the proposer payload' CODEC(ZSTD(1)),
    `builder_pubkey` String COMMENT 'The builder pubkey of the proposer payload' CODEC(ZSTD(1)),
    `proposer_pubkey` String COMMENT 'The proposer pubkey of the proposer payload' CODEC(ZSTD(1)),
    `proposer_fee_recipient` FixedString(42) COMMENT 'The proposer fee recipient of the proposer payload' CODEC(ZSTD(1)),
    `gas_limit` UInt64 COMMENT 'The gas limit of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `gas_used` UInt64 COMMENT 'The gas used of the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `value` Nullable(UInt128) COMMENT 'The transaction value in wei' CODEC(ZSTD(1)),
    `transaction_count` UInt32 COMMENT 'The number of transactions in the proposer payload' CODEC(DoubleDelta, ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Can be "canonical" or "orphaned"'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'MEV relay proposer payload delivered for a block on the finalized chain including orphaned blocks';

CREATE TABLE `${NETWORK_NAME}`.fct_block_mev ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_block_mev_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_mev_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_block_mev_local
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_mev_bid_by_builder_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number within the block bid' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time for the slot that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The start time for the epoch that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `earliest_bid_date_time` DateTime64(3) COMMENT 'The timestamp of the earliest bid in milliseconds' CODEC(DoubleDelta, ZSTD(1)),
    `relay_names` Array(String) COMMENT 'The relay that the bid was fetched from' CODEC(ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'The execution block hash of the bid' CODEC(ZSTD(1)),
    `builder_pubkey` String COMMENT 'The builder pubkey of the bid' CODEC(ZSTD(1)),
    `value` UInt128 COMMENT 'The transaction value in wei' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_hash`, `builder_pubkey`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'All unique bids from builders for a slot';

CREATE TABLE `${NETWORK_NAME}`.fct_mev_bid_by_builder ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_mev_bid_by_builder_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_mev_bid_by_builder_local,
    cityHash64(`slot_start_date_time`, `block_hash`, `builder_pubkey`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_mev_bid_by_builder_local
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_hash`, `builder_pubkey`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_mev_bid_count_by_relay_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number within the block bid' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time for the slot that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The start time for the epoch that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `relay_name` String COMMENT 'The relay that the bid was fetched from' CODEC(ZSTD(1)),
    `bid_total` UInt32 COMMENT 'The total number of bids for the relay' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `relay_name`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Total number of MEV relay bids for a slot by relay';

CREATE TABLE `${NETWORK_NAME}`.fct_mev_bid_count_by_relay ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_mev_bid_count_by_relay_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_mev_bid_count_by_relay_local,
    cityHash64(`slot_start_date_time`, `relay_name`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_mev_bid_count_by_relay_local
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `relay_name`)
);
