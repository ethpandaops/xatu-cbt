CREATE TABLE `${NETWORK_NAME}`.int_engine_new_payload_local ON CLUSTER '{cluster}' (
    -- Metadata
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),

    -- Timing (from raw)
    `event_date_time` DateTime64(3) COMMENT 'When the sentry received the event' CODEC(DoubleDelta, ZSTD(1)),
    `requested_date_time` DateTime64(3) COMMENT 'When the engine_newPayload call was initiated' CODEC(DoubleDelta, ZSTD(1)),
    `duration_ms` UInt64 COMMENT 'How long the engine_newPayload call took in milliseconds' CODEC(ZSTD(1)),

    -- Time dimensions (from raw)
    `slot` UInt32 COMMENT 'Slot number of the beacon block containing the payload' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'Epoch number derived from the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),

    -- Block identifiers (from raw)
    `block_root` FixedString(66) COMMENT 'Root of the beacon block (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `block_hash` FixedString(66) COMMENT 'Execution block hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `block_number` UInt64 COMMENT 'Execution block number' CODEC(DoubleDelta, ZSTD(1)),
    `parent_block_root` FixedString(66) COMMENT 'Root of the parent beacon block (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `parent_hash` FixedString(66) COMMENT 'Parent execution block hash (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `proposer_index` UInt32 COMMENT 'Validator index of the block proposer' CODEC(ZSTD(1)),

    -- Block metrics (from raw)
    `gas_used` UInt64 COMMENT 'Total gas used by all transactions in the block' CODEC(ZSTD(1)),
    `gas_limit` UInt64 COMMENT 'Gas limit of the block' CODEC(ZSTD(1)),
    `tx_count` UInt32 COMMENT 'Number of transactions in the block' CODEC(ZSTD(1)),
    `blob_count` UInt32 COMMENT 'Number of blobs in the block' CODEC(ZSTD(1)),

    -- Engine API status (from raw)
    `status` LowCardinality(String) COMMENT 'Engine API response status (VALID, INVALID, SYNCING, ACCEPTED, INVALID_BLOCK_HASH, ERROR)' CODEC(ZSTD(1)),
    `validation_error` Nullable(String) COMMENT 'Error message when validation fails' CODEC(ZSTD(1)),
    `latest_valid_hash` Nullable(FixedString(66)) COMMENT 'Latest valid hash when status is INVALID (hex encoded with 0x prefix)' CODEC(ZSTD(1)),
    `method_version` LowCardinality(String) COMMENT 'Version of the engine_newPayload method (e.g., V3, V4)' CODEC(ZSTD(1)),

    -- From fct_block_head (ENRICHMENT)
    `block_total_bytes` Nullable(UInt32) COMMENT 'The total bytes of the beacon block payload' CODEC(ZSTD(1)),
    `block_total_bytes_compressed` Nullable(UInt32) COMMENT 'The total bytes of the beacon block payload when compressed using snappy' CODEC(ZSTD(1)),
    `block_version` LowCardinality(String) COMMENT 'The version of the beacon block (phase0, altair, bellatrix, capella, deneb)' CODEC(ZSTD(1)),

    -- Node classification (derived)
    `node_class` LowCardinality(String) COMMENT 'Node classification for grouping observations (e.g., eip7870-block-builder, or empty for general nodes)' CODEC(ZSTD(1)),

    -- Client metadata (from raw)
    `meta_execution_version` LowCardinality(String) COMMENT 'Full execution client version string from web3_clientVersion RPC' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation name (e.g., Geth, Nethermind, Besu, Reth)' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Implementation of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Version of the client that generated the event' CODEC(ZSTD(1)),

    -- Geo metadata (from raw)
    `meta_client_geo_city` LowCardinality(String) COMMENT 'City of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_country` LowCardinality(String) COMMENT 'Country of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_country_code` LowCardinality(String) COMMENT 'Country code of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_continent_code` LowCardinality(String) COMMENT 'Continent code of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_latitude` Nullable(Float64) COMMENT 'Latitude of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_longitude` Nullable(Float64) COMMENT 'Longitude of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_number` Nullable(UInt32) COMMENT 'Autonomous system number of the client that generated the event' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_organization` Nullable(String) COMMENT 'Autonomous system organization of the client that generated the event' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (slot_start_date_time, block_hash, meta_client_name, event_date_time)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Individual engine_newPayload observations enriched with block size from fct_block_head';

CREATE TABLE `${NETWORK_NAME}`.int_engine_new_payload ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.int_engine_new_payload_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    int_engine_new_payload_local,
    cityHash64(slot_start_date_time, block_hash, meta_client_name, event_date_time)
);

ALTER TABLE `${NETWORK_NAME}`.int_engine_new_payload_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (slot, block_hash)
);

ALTER TABLE `${NETWORK_NAME}`.int_engine_new_payload_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_duration_ms
(
    SELECT *
    ORDER BY (duration_ms, slot_start_date_time)
);
