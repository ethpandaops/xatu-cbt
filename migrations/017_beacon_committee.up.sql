CREATE TABLE `${NETWORK_NAME}`.int_beacon_committee_head_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `committee_index` LowCardinality(String) COMMENT 'The committee index in the beacon API committee payload',
    `validators` Array(UInt32) COMMENT 'The validator indices in the beacon API committee payload' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `committee_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 384,
    min_age_to_force_merge_on_partition_only=false
COMMENT 'Beacon committee head for the unfinalized chain';

CREATE TABLE `${NETWORK_NAME}`.int_beacon_committee_head ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_beacon_committee_head_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_beacon_committee_head_local,
    cityHash64(`slot_start_date_time`, `committee_index`)
);
