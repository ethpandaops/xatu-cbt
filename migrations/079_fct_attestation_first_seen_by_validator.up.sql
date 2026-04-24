CREATE TABLE `${NETWORK_NAME}`.fct_attestation_first_seen_by_validator_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot the validator was attesting for' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `validator_index` UInt32 COMMENT 'The validator index' CODEC(ZSTD(1)),
    `committee_index` LowCardinality(String) COMMENT 'The committee the validator was assigned to for this slot',
    `raw_seen_slot_start_diff` Nullable(UInt32) COMMENT 'Earliest time (ms after slot start) the unaggregated attestation from this validator was seen. NULL if never seen raw.' CODEC(ZSTD(1)),
    `raw_source` LowCardinality(String) COMMENT 'Source the raw attestation was first observed from (beacon_api, libp2p, or empty if never seen raw)' CODEC(ZSTD(1)),
    `raw_block_root` String COMMENT 'Head vote from the earliest raw attestation (empty if never seen raw)' CODEC(ZSTD(1)),
    `raw_source_epoch` UInt32 COMMENT 'Source checkpoint epoch from the earliest raw attestation' CODEC(DoubleDelta, ZSTD(1)),
    `raw_source_root` String COMMENT 'Source checkpoint root from the earliest raw attestation' CODEC(ZSTD(1)),
    `raw_target_epoch` UInt32 COMMENT 'Target checkpoint epoch from the earliest raw attestation' CODEC(DoubleDelta, ZSTD(1)),
    `raw_target_root` String COMMENT 'Target checkpoint root from the earliest raw attestation' CODEC(ZSTD(1)),
    `agg_seen_slot_start_diff` Nullable(UInt32) COMMENT 'Earliest time (ms after slot start) the validator was seen inside an aggregate. NULL if never seen in an aggregate.' CODEC(ZSTD(1)),
    `agg_source` LowCardinality(String) COMMENT 'Source the earliest aggregate was observed from (beacon_api, libp2p, or empty if never seen)' CODEC(ZSTD(1)),
    `agg_block_root` String COMMENT 'Head vote from the earliest aggregate containing this validator' CODEC(ZSTD(1)),
    `agg_source_epoch` UInt32 COMMENT 'Source checkpoint epoch from the earliest aggregate containing this validator' CODEC(DoubleDelta, ZSTD(1)),
    `agg_source_root` String COMMENT 'Source checkpoint root from the earliest aggregate containing this validator' CODEC(ZSTD(1)),
    `agg_target_epoch` UInt32 COMMENT 'Target checkpoint epoch from the earliest aggregate containing this validator' CODEC(DoubleDelta, ZSTD(1)),
    `agg_target_root` String COMMENT 'Target checkpoint root from the earliest aggregate containing this validator' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `validator_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Per-(slot, validator) first-seen timings for both the unaggregated attestation and the earliest aggregate that included the validator, with the attested votes from each source.';

CREATE TABLE `${NETWORK_NAME}`.fct_attestation_first_seen_by_validator ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_attestation_first_seen_by_validator_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_attestation_first_seen_by_validator_local,
    cityHash64(`slot_start_date_time`, `validator_index`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_attestation_first_seen_by_validator_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `validator_index`)
);
