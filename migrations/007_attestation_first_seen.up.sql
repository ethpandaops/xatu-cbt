CREATE TABLE `${NETWORK_NAME}`.int_attestation_first_seen_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `source` LowCardinality(String) COMMENT 'Source of the event' CODEC(ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `seen_slot_start_diff` UInt32 COMMENT 'The time from slot start for the client to see the block' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `attesting_validator_index` UInt32 COMMENT 'The index of the validator attesting' CODEC(ZSTD(1)),
    `attesting_validator_committee_index` LowCardinality(String) COMMENT 'The committee index of the attesting validator',
    `username` LowCardinality(String) COMMENT 'Username of the node' CODEC(ZSTD(1)),
    `node_id` String COMMENT 'ID of the node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the client',
    `meta_client_version` LowCardinality(String) COMMENT 'Version of the client',
    `meta_client_implementation` LowCardinality(String) COMMENT 'Implementation of the client',
    `meta_client_geo_city` LowCardinality(String) COMMENT 'City of the client' CODEC(ZSTD(1)),
    `meta_client_geo_country` LowCardinality(String) COMMENT 'Country of the client' CODEC(ZSTD(1)),
    `meta_client_geo_country_code` LowCardinality(String) COMMENT 'Country code of the client' CODEC(ZSTD(1)),
    `meta_client_geo_continent_code` LowCardinality(String) COMMENT 'Continent code of the client' CODEC(ZSTD(1)),
    `meta_client_geo_longitude` Nullable(Float64) COMMENT 'Longitude of the client' CODEC(ZSTD(1)),
    `meta_client_geo_latitude` Nullable(Float64) COMMENT 'Latitude of the client' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_number` Nullable(UInt32) COMMENT 'Autonomous system number of the client' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_organization` Nullable(String) COMMENT 'Autonomous system organization of the client' CODEC(ZSTD(1)),
    `meta_consensus_version` LowCardinality(String) COMMENT 'Ethereum consensus client version',
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Ethereum consensus client implementation'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `attesting_validator_index`)
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
COMMENT 'When the attestation was first seen on the network by a sentry node';

CREATE TABLE `${NETWORK_NAME}`.int_attestation_first_seen ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.int_attestation_first_seen_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_attestation_first_seen_local,
    cityHash64(`slot_start_date_time`, `attesting_validator_index`)
);
