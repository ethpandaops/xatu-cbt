CREATE TABLE `${NETWORK_NAME}`.fct_engine_new_payload_by_node_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'Slot number of the beacon block containing the payload' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` FixedString(66) COMMENT 'Root of the beacon block (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `parent_block_root` FixedString(66) COMMENT 'Root of the parent beacon block (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `proposer_index` UInt32 COMMENT 'Validator index of the block proposer' CODEC(ZSTD(1)),
    `block_number` UInt64 COMMENT 'Execution block number' CODEC(DoubleDelta, ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'Execution block hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `parent_hash` FixedString(66) COMMENT 'Parent execution block hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `gas_used` UInt64 COMMENT 'Total gas used by all transactions in the block' CODEC(ZSTD(1)),
    `gas_limit` UInt64 COMMENT 'Gas limit of the block' CODEC(ZSTD(1)),
    `tx_count` UInt32 COMMENT 'Number of transactions in the block' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'Number of blobs in the block' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Payload status returned by EL (VALID, INVALID, SYNCING, ACCEPTED, INVALID_BLOCK_HASH)',
    `latest_valid_hash` Nullable(FixedString(66)) COMMENT 'Latest valid hash when status is INVALID (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `validation_error` Nullable(String) COMMENT 'Error message when validation fails' CODEC(ZSTD(1)),
    `duration_ms` UInt64 COMMENT 'How long the engine_newPayload call took in milliseconds' CODEC(ZSTD(1)),
    `method_version` LowCardinality(String) COMMENT 'Version of the engine_newPayload method (e.g., V3, V4)',
    `username` String COMMENT 'Username extracted from meta_client_name' CODEC(ZSTD(1)),
    `node_id` String COMMENT 'Node ID extracted from meta_client_name' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Node classification (individual, corporate, internal, unclassified)',
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the client that generated the event',
    `meta_client_version` LowCardinality(String) COMMENT 'Version of the client that generated the event',
    `meta_client_implementation` LowCardinality(String) COMMENT 'Consensus client implementation (e.g., lighthouse, prysm, teku)',
    `meta_execution_version` LowCardinality(String) COMMENT 'Full execution client version string',
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation (e.g., Geth, Nethermind, Besu, Reth)',
    `meta_execution_version_major` LowCardinality(String) COMMENT 'Execution client major version number',
    `meta_execution_version_minor` LowCardinality(String) COMMENT 'Execution client minor version number',
    `meta_execution_version_patch` LowCardinality(String) COMMENT 'Execution client patch version number',
    `meta_client_geo_city` LowCardinality(String) COMMENT 'City of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_country` LowCardinality(String) COMMENT 'Country of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_country_code` LowCardinality(String) COMMENT 'Country code of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_continent_code` LowCardinality(String) COMMENT 'Continent code of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_longitude` Nullable(Float64) COMMENT 'Longitude of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_latitude` Nullable(Float64) COMMENT 'Latitude of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_number` Nullable(UInt32) COMMENT 'Autonomous system number of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_organization` Nullable(String) COMMENT 'Autonomous system organization of the client that generated the event' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (slot_start_date_time, meta_client_name, block_hash)
COMMENT 'Per-node engine_newPayload call observations with timing and status';

CREATE TABLE `${NETWORK_NAME}`.fct_engine_new_payload_by_node ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_engine_new_payload_by_node_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_engine_new_payload_by_node_local,
    cityHash64(slot_start_date_time, meta_client_name, block_hash)
);
