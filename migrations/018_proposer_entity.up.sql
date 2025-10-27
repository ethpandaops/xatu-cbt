CREATE TABLE `${NETWORK_NAME}`.fct_block_proposer_entity_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `entity` Nullable(String) COMMENT 'The entity that proposed the block' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    min_age_to_force_merge_seconds = 4,
    min_age_to_force_merge_on_partition_only=false,
    max_replicated_merges_in_queue = 64,
    max_replicated_merges_with_ttl_in_queue = 32
    number_of_free_entries_in_pool_to_lower_max_size_of_merge = 8,
    max_bytes_to_merge_at_min_space_in_pool = 512e6,
    max_bytes_to_merge_at_max_space_in_pool = 8e9,
    parts_to_delay_insert = 300,
    parts_to_throw_insert = 600,
    merge_max_block_size = 8192
COMMENT 'Block proposer entity for the unfinalized chain';

CREATE TABLE `${NETWORK_NAME}`.fct_block_proposer_entity ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_block_proposer_entity_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_block_proposer_entity_local,
    cityHash64(`slot_start_date_time`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_block_proposer_entity_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`)
);
