CREATE TABLE `${NETWORK_NAME}`.dim_node_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `validator_index` UInt32 COMMENT 'The index of the validator' CODEC(ZSTD(1)),
    `name` Nullable(String) COMMENT 'The name of the node' CODEC(ZSTD(1)),
    `groups` Array(String) COMMENT 'Groups the node belongs to' CODEC(ZSTD(1)),
    `tags` Array(String) COMMENT 'Tags associated with the node' CODEC(ZSTD(1)),
    `attributes` Map(String, String) COMMENT 'Additional attributes of the node' CODEC(ZSTD(1)),
    `source` String COMMENT 'The source entity of the node' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY
    (`validator_index`)
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
COMMENT 'Node information for validators';

CREATE TABLE `${NETWORK_NAME}`.dim_node ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.dim_node_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    dim_node_local,
    cityHash64(`validator_index`)
);