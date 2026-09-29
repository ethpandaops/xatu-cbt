CREATE TABLE fct_block_payload_ptc_vote_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The attested slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the attested slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the attested slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root being attested by the PTC' CODEC(ZSTD(1)),
    `ptc_validators_seen` UInt32 COMMENT 'Distinct PTC validators whose payload attestation was seen on the live event stream' CODEC(DoubleDelta, ZSTD(1)),
    `payload_present_votes` UInt32 COMMENT 'Distinct PTC validators attesting the payload was present' CODEC(DoubleDelta, ZSTD(1)),
    `blob_data_available_votes` UInt32 COMMENT 'Distinct PTC validators attesting blob data was available' CODEC(DoubleDelta, ZSTD(1)),
    `first_seen_slot_start_diff` UInt32 COMMENT 'Time from slot start that the first payload attestation was seen in ms' CODEC(DoubleDelta, ZSTD(1)),
    `last_seen_slot_start_diff` UInt32 COMMENT 'Time from slot start that the last payload attestation was seen in ms' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Gloas (ePBS) Payload Timeliness Committee votes per attested block, observed on the live beacon API event stream. Available at head without waiting for finalization.';

CREATE TABLE fct_block_payload_ptc_vote_head ON CLUSTER '{cluster}' AS fct_block_payload_ptc_vote_head_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_payload_ptc_vote_head_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE fct_block_payload_ptc_vote_head_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);

CREATE TABLE int_block_payload_ptc_vote_canonical_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The attested slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the attested slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the attested slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root being attested by the PTC' CODEC(ZSTD(1)),
    `block_version` LowCardinality(String) COMMENT 'The beacon block version of the containing block' CODEC(ZSTD(1)),
    `included_in_slot` UInt32 COMMENT 'Slot of the canonical block that included the payload attestations' CODEC(DoubleDelta, ZSTD(1)),
    `included_in_block_root` String COMMENT 'Root of the canonical block that included the payload attestations' CODEC(ZSTD(1)),
    `ptc_validators` UInt32 COMMENT 'Total PTC validators covered by the included payload attestation aggregates' CODEC(DoubleDelta, ZSTD(1)),
    `payload_present_votes` UInt32 COMMENT 'PTC validators attesting the payload was present' CODEC(DoubleDelta, ZSTD(1)),
    `blob_data_available_votes` UInt32 COMMENT 'PTC validators attesting blob data was available' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Gloas (ePBS) Payload Timeliness Committee votes per attested block, from aggregates included in canonical beacon blocks';

CREATE TABLE int_block_payload_ptc_vote_canonical ON CLUSTER '{cluster}' AS int_block_payload_ptc_vote_canonical_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_block_payload_ptc_vote_canonical_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

CREATE TABLE fct_block_payload_ptc_vote_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The attested slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the attested slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the attested slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root being attested by the PTC' CODEC(ZSTD(1)),
    `block_version` LowCardinality(String) COMMENT 'The beacon block version of the containing block, empty for orphaned rows' CODEC(ZSTD(1)),
    `included_in_slot` Nullable(UInt32) COMMENT 'Slot of the canonical block that included the payload attestations, null for orphaned rows' CODEC(ZSTD(1)),
    `included_in_block_root` Nullable(String) COMMENT 'Root of the canonical block that included the payload attestations, null for orphaned rows' CODEC(ZSTD(1)),
    `ptc_validators` UInt32 COMMENT 'Total PTC validators covered: on-chain aggregate counts for canonical rows, distinct validators seen on the live stream for orphaned rows' CODEC(DoubleDelta, ZSTD(1)),
    `payload_present_votes` UInt32 COMMENT 'PTC validators attesting the payload was present' CODEC(DoubleDelta, ZSTD(1)),
    `blob_data_available_votes` UInt32 COMMENT 'PTC validators attesting blob data was available' CODEC(DoubleDelta, ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Whether the attested block is canonical or orphaned' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Gloas (ePBS) Payload Timeliness Committee verdict per attested block: canonical on-chain aggregates unioned with orphaned blocks only seen on the live stream';

CREATE TABLE fct_block_payload_ptc_vote ON CLUSTER '{cluster}' AS fct_block_payload_ptc_vote_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_payload_ptc_vote_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE fct_block_payload_ptc_vote_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);
