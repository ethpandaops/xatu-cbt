CREATE TABLE `${NETWORK_NAME}`.fct_mev_relay_bid_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number within the block bid' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The start time for the slot that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The start time for the epoch that the bid is for' CODEC(DoubleDelta, ZSTD(1)),
    `requested_at_slot_time` UInt32 COMMENT 'The time in the slot when the request was sent' CODEC(ZSTD(1)),
    `response_at_slot_time` UInt32 COMMENT 'The time in the slot when the response was received' CODEC(ZSTD(1)),
    `bid_date_time` DateTime64(3) COMMENT 'The timestamp of the bid in milliseconds' CODEC(DoubleDelta, ZSTD(1)),
    `relay_name` String COMMENT 'The relay that the bid was fetched from' CODEC(ZSTD(1)),
    `parent_hash` FixedString(66) COMMENT 'The parent hash of the bid' CODEC(ZSTD(1)),
    `block_number` UInt64 COMMENT 'The block number of the bid' CODEC(DoubleDelta, ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'The block hash of the bid' CODEC(ZSTD(1)),
    `builder_pubkey` String COMMENT 'The builder pubkey of the bid' CODEC(ZSTD(1)),
    `proposer_pubkey` String COMMENT 'The proposer pubkey of the bid' CODEC(ZSTD(1)),
    `proposer_fee_recipient` FixedString(42) COMMENT 'The proposer fee recipient of the bid' CODEC(ZSTD(1)),
    `gas_limit` UInt64 COMMENT 'The gas limit of the bid' CODEC(DoubleDelta, ZSTD(1)),
    `gas_used` UInt64 COMMENT 'The gas used of the bid' CODEC(DoubleDelta, ZSTD(1)),
    `value` UInt256 COMMENT 'The transaction value in float64' CODEC(ZSTD(1)),
    `transaction_count` UInt32 COMMENT 'The number of transactions in the bid' CODEC(DoubleDelta, ZSTD(1)),
    `optimistic_submission` Bool COMMENT 'Whether the bid was optimistic' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `relay_name`, `block_hash`)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Block details for the finalized chain including orphaned blocks';

CREATE TABLE `${NETWORK_NAME}`.fct_mev_relay_bid ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_mev_relay_bid_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_mev_relay_bid_local,
    cityHash64(`slot_start_date_time`, `relay_name`, `block_hash`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_mev_relay_bid_local
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `relay_name`, `block_hash`)
);
