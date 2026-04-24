CREATE TABLE `${NETWORK_NAME}`.int_attestation_first_seen_aggregate_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `attesting_validator_index` UInt32 COMMENT 'The index of the validator attesting' CODEC(ZSTD(1)),
    `committee_index` LowCardinality(String) COMMENT 'The committee index the validator is assigned to',
    `seen_slot_start_diff` UInt32 COMMENT 'The earliest time (ms after slot start) the validator was seen attesting inside an aggregate' CODEC(DoubleDelta, ZSTD(1)),
    `source` LowCardinality(String) COMMENT 'The first source this aggregate was observed from (beacon_api or libp2p)' CODEC(ZSTD(1)),
    `block_root` String COMMENT 'The head vote (beacon block root) from the earliest aggregate' CODEC(ZSTD(1)),
    `source_epoch` UInt32 COMMENT 'Source checkpoint epoch from the earliest aggregate' CODEC(DoubleDelta, ZSTD(1)),
    `source_root` String COMMENT 'Source checkpoint root from the earliest aggregate' CODEC(ZSTD(1)),
    `target_epoch` UInt32 COMMENT 'Target checkpoint epoch from the earliest aggregate' CODEC(DoubleDelta, ZSTD(1)),
    `target_root` String COMMENT 'Target checkpoint root from the earliest aggregate' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `attesting_validator_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'When each validator was first seen inside an aggregate attestation, derived by decoding aggregation_bits against canonical committee assignments';

CREATE TABLE `${NETWORK_NAME}`.int_attestation_first_seen_aggregate ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_attestation_first_seen_aggregate_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_attestation_first_seen_aggregate_local,
    cityHash64(`slot_start_date_time`, `attesting_validator_index`)
);
