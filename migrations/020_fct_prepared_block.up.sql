CREATE TABLE `${NETWORK_NAME}`.fct_prepared_block_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number from beacon block',
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started',
    `event_date_time` DateTime COMMENT 'The wall clock time when the event was received',
    `meta_client_name` String COMMENT 'Name of the client that generated the event',
    `meta_client_version` String COMMENT 'Version of the client that generated the event',
    `meta_client_implementation` String COMMENT 'Implementation of the client that generated the event',
    `meta_consensus_implementation` String COMMENT 'Consensus implementation of the validator',
    `meta_consensus_version` String COMMENT 'Consensus version of the validator',
    `meta_client_geo_city` String COMMENT 'City of the client that generated the event',
    `meta_client_geo_country` String COMMENT 'Country of the client that generated the event',
    `meta_client_geo_country_code` String COMMENT 'Country code of the client that generated the event',
    `block_version` LowCardinality(String) COMMENT 'The version of the beacon block',
    `block_total_bytes` Nullable(UInt32) COMMENT 'The total bytes of the beacon block payload',
    `block_total_bytes_compressed` Nullable(UInt32) COMMENT 'The total bytes of the beacon block payload when compressed using snappy',
    `execution_payload_value` Nullable(UInt64) COMMENT 'The value of the execution payload in wei',
    `consensus_payload_value` Nullable(UInt64) COMMENT 'The value of the consensus payload in wei',
    `execution_payload_block_number` UInt32 COMMENT 'The block number of the execution payload',
    `execution_payload_gas_limit` Nullable(UInt64) COMMENT 'Gas limit for execution payload',
    `execution_payload_gas_used` Nullable(UInt64) COMMENT 'Gas used for execution payload',
    `execution_payload_transactions_count` Nullable(UInt32) COMMENT 'The transaction count of the execution payload',
    `execution_payload_transactions_total_bytes` Nullable(UInt32) COMMENT 'The transaction total bytes of the execution payload'
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `slot`, `meta_client_name`, `event_date_time`)
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
COMMENT 'Prepared block proposals showing what would have been built if the validator had been selected as proposer';


CREATE TABLE `${NETWORK_NAME}`.fct_prepared_block ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.fct_prepared_block_local ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_prepared_block_local,
    cityHash64(`slot_start_date_time`, `slot`, `meta_client_name`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_prepared_block_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `meta_client_name`, `event_date_time`)
);

ALTER TABLE `${NETWORK_NAME}`.fct_prepared_block_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_client
(
    SELECT *
    ORDER BY (`meta_client_name`, `slot_start_date_time`, `slot`)
);