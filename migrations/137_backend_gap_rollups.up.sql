CREATE TABLE fct_block_graffiti_client_version_pair_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `consensus_client` LowCardinality(String) COMMENT 'Consensus client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `consensus_version` LowCardinality(String) COMMENT 'Consensus client release version the graffiti resolves to, or the version it states when unresolved, empty when there is none' CODEC(ZSTD(1)),
    `consensus_release_status` LowCardinality(String) COMMENT 'How the consensus client version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `execution_client` LowCardinality(String) COMMENT 'Execution client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `execution_version` LowCardinality(String) COMMENT 'Execution client release version the graffiti resolves to, or the version it states when unresolved, empty when there is none' CODEC(ZSTD(1)),
    `execution_release_status` LowCardinality(String) COMMENT 'How the execution client version was resolved, see the table comment for values' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks with this consensus and execution client release pair' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `consensus_client`, `execution_client`, `consensus_version`, `execution_version`, `consensus_release_status`, `execution_release_status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily canonical block counts per consensus and execution client release pair resolved from block graffiti, each block counted once, with builds of one version from different commits together. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). An unresolved side keeps the version its graffiti states, or is empty when it states none';

CREATE TABLE fct_block_graffiti_client_version_pair_daily ON CLUSTER '{cluster}' AS fct_block_graffiti_client_version_pair_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_version_pair_daily_local,
    cityHash64(`day_start_date`, `consensus_client`, `execution_client`, `consensus_version`, `execution_version`, `consensus_release_status`, `execution_release_status`)
);

CREATE TABLE fct_block_payload_status_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'PTC verdict bucket: delivered or absent' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Number of blocks with this payload outcome in the day' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `status`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Gloas (ePBS) daily payload delivery outcomes judged by the PTC, the daily twin of fct_block_payload_status_hourly. A block counts as delivered when a majority of observed PTC votes said the payload was present, absent otherwise';

CREATE TABLE fct_block_payload_status_daily ON CLUSTER '{cluster}' AS fct_block_payload_status_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_payload_status_daily_local,
    cityHash64(`day_start_date`, `status`)
);

CREATE TABLE fct_engine_new_payload_duration_chunked_50ms_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification for grouping observations (e.g., eip7870-block-builder, or empty for general nodes)' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation name (e.g., Geth, Nethermind, Besu, Reth)' CODEC(ZSTD(1)),
    `meta_execution_version` LowCardinality(String) COMMENT 'Full execution client version string from web3_clientVersion RPC' CODEC(ZSTD(1)),
    `chunk_duration_ms` Int64 COMMENT 'Duration bucket in 50ms chunks (0, 50, 100, 150, ...)' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of engine_newPayload observations in this duration chunk' CODEC(ZSTD(1)),
    `valid_count` UInt32 COMMENT 'Number of VALID status observations in this chunk' CODEC(ZSTD(1)),
    `invalid_count` UInt32 COMMENT 'Number of INVALID or INVALID_BLOCK_HASH status observations in this chunk' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `chunk_duration_ms`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly engine_newPayload duration distribution in 50ms chunks per node class and execution client version, from int_engine_new_payload, for latency histograms and percentiles over long ranges';

CREATE TABLE fct_engine_new_payload_duration_chunked_50ms_hourly ON CLUSTER '{cluster}' AS fct_engine_new_payload_duration_chunked_50ms_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_duration_chunked_50ms_hourly_local,
    cityHash64(`hour_start_date_time`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `chunk_duration_ms`)
);

CREATE TABLE fct_engine_new_payload_duration_chunked_50ms_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification for grouping observations (e.g., eip7870-block-builder, or empty for general nodes)' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation name (e.g., Geth, Nethermind, Besu, Reth)' CODEC(ZSTD(1)),
    `meta_execution_version` LowCardinality(String) COMMENT 'Full execution client version string from web3_clientVersion RPC' CODEC(ZSTD(1)),
    `chunk_duration_ms` Int64 COMMENT 'Duration bucket in 50ms chunks (0, 50, 100, 150, ...)' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of engine_newPayload observations in this duration chunk' CODEC(ZSTD(1)),
    `valid_count` UInt32 COMMENT 'Number of VALID status observations in this chunk' CODEC(ZSTD(1)),
    `invalid_count` UInt32 COMMENT 'Number of INVALID or INVALID_BLOCK_HASH status observations in this chunk' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `chunk_duration_ms`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily engine_newPayload duration distribution in 50ms chunks per node class and execution client version, from int_engine_new_payload, for latency histograms and percentiles over long ranges';

CREATE TABLE fct_engine_new_payload_duration_chunked_50ms_daily ON CLUSTER '{cluster}' AS fct_engine_new_payload_duration_chunked_50ms_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_duration_chunked_50ms_daily_local,
    cityHash64(`day_start_date`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `chunk_duration_ms`)
);

CREATE TABLE fct_engine_get_blobs_duration_chunked_50ms_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification for grouping observations (e.g., eip7870-block-builder, or empty for general nodes)' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation name (e.g., Geth, Nethermind, Besu, Reth)' CODEC(ZSTD(1)),
    `meta_execution_version` LowCardinality(String) COMMENT 'Full execution client version string from web3_clientVersion RPC' CODEC(ZSTD(1)),
    `chunk_duration_ms` Int64 COMMENT 'Duration bucket in 50ms chunks (0, 50, 100, 150, ...)' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of engine_getBlobs observations in this duration chunk' CODEC(ZSTD(1)),
    `success_count` UInt32 COMMENT 'Number of SUCCESS status observations in this chunk' CODEC(ZSTD(1)),
    `partial_count` UInt32 COMMENT 'Number of PARTIAL status observations in this chunk' CODEC(ZSTD(1)),
    `empty_count` UInt32 COMMENT 'Number of EMPTY status observations in this chunk' CODEC(ZSTD(1)),
    `error_count` UInt32 COMMENT 'Number of ERROR or UNSUPPORTED status observations in this chunk' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `chunk_duration_ms`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly engine_getBlobs duration distribution in 50ms chunks per node class and execution client version, from int_engine_get_blobs, for latency histograms and percentiles over long ranges';

CREATE TABLE fct_engine_get_blobs_duration_chunked_50ms_hourly ON CLUSTER '{cluster}' AS fct_engine_get_blobs_duration_chunked_50ms_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_get_blobs_duration_chunked_50ms_hourly_local,
    cityHash64(`hour_start_date_time`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `chunk_duration_ms`)
);

CREATE TABLE fct_engine_get_blobs_duration_chunked_50ms_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period, by slot' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification for grouping observations (e.g., eip7870-block-builder, or empty for general nodes)' CODEC(ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation name (e.g., Geth, Nethermind, Besu, Reth)' CODEC(ZSTD(1)),
    `meta_execution_version` LowCardinality(String) COMMENT 'Full execution client version string from web3_clientVersion RPC' CODEC(ZSTD(1)),
    `chunk_duration_ms` Int64 COMMENT 'Duration bucket in 50ms chunks (0, 50, 100, 150, ...)' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of engine_getBlobs observations in this duration chunk' CODEC(ZSTD(1)),
    `success_count` UInt32 COMMENT 'Number of SUCCESS status observations in this chunk' CODEC(ZSTD(1)),
    `partial_count` UInt32 COMMENT 'Number of PARTIAL status observations in this chunk' CODEC(ZSTD(1)),
    `empty_count` UInt32 COMMENT 'Number of EMPTY status observations in this chunk' CODEC(ZSTD(1)),
    `error_count` UInt32 COMMENT 'Number of ERROR or UNSUPPORTED status observations in this chunk' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `chunk_duration_ms`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily engine_getBlobs duration distribution in 50ms chunks per node class and execution client version, from int_engine_get_blobs, for latency histograms and percentiles over long ranges';

CREATE TABLE fct_engine_get_blobs_duration_chunked_50ms_daily ON CLUSTER '{cluster}' AS fct_engine_get_blobs_duration_chunked_50ms_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_get_blobs_duration_chunked_50ms_daily_local,
    cityHash64(`day_start_date`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `chunk_duration_ms`)
);

CREATE TABLE fct_epoch_summary_canonical_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `slot_count` UInt8 COMMENT 'Slots of the epoch with a proposer duty or block, 32 once the epoch is complete' CODEC(ZSTD(1)),
    `canonical_block_count` UInt8 COMMENT 'Slots with a block on the finalized chain' CODEC(ZSTD(1)),
    `missed_slot_count` UInt8 COMMENT 'Slots without a block on the finalized chain, whether the proposer missed or the block was orphaned' CODEC(ZSTD(1)),
    `votes_max` UInt32 COMMENT 'Attestations scheduled across the epoch slots' CODEC(ZSTD(1)),
    `votes_head` UInt32 COMMENT 'Attestations included on chain that voted for the block of their own slot' CODEC(ZSTD(1)),
    `votes_other` UInt32 COMMENT 'Attestations included on chain that voted for another block' CODEC(ZSTD(1)),
    `participation_rate` Nullable(Float64) COMMENT 'Share of scheduled attestations included on chain, (votes_head + votes_other) / votes_max from 0 to 1, NULL when none were scheduled' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(epoch_start_date_time)
ORDER BY
    (`epoch_start_date_time`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'One row per epoch of the finalized chain with its proposer duties, canonical blocks, slots without a canonical block and attestation participation, from int_block_proposer_canonical and fct_attestation_correctness_canonical';

CREATE TABLE fct_epoch_summary_canonical ON CLUSTER '{cluster}' AS fct_epoch_summary_canonical_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_epoch_summary_canonical_local,
    cityHash64(`epoch_start_date_time`)
);

CREATE TABLE fct_node_last_seen_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `last_seen_date_time` DateTime COMMENT 'Start of the latest slot the Xatu node reported a block in' CODEC(DoubleDelta, ZSTD(1)),
    `username` LowCardinality(String) COMMENT 'Username of the Xatu node' CODEC(ZSTD(1)),
    `node_id` String COMMENT 'ID of the Xatu node' CODEC(ZSTD(1)),
    `classification` LowCardinality(String) COMMENT 'Classification of the Xatu node, e.g. "individual", "corporate", "internal" (aka ethPandaOps) or "unclassified"' CODEC(ZSTD(1)),
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the Xatu node' CODEC(ZSTD(1)),
    `meta_client_version` LowCardinality(String) COMMENT 'Version of the collector at the latest observation' CODEC(ZSTD(1)),
    `meta_client_implementation` LowCardinality(String) COMMENT 'Implementation of the collector at the latest observation' CODEC(ZSTD(1)),
    `meta_client_geo_city` LowCardinality(String) COMMENT 'City of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_client_geo_country` LowCardinality(String) COMMENT 'Country of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_client_geo_country_code` LowCardinality(String) COMMENT 'Country code of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_client_geo_continent_code` LowCardinality(String) COMMENT 'Continent code of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_client_geo_longitude` Nullable(Float64) COMMENT 'Longitude of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_client_geo_latitude` Nullable(Float64) COMMENT 'Latitude of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_number` Nullable(UInt32) COMMENT 'Autonomous system number of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_client_geo_autonomous_system_organization` Nullable(String) COMMENT 'Autonomous system organization of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_consensus_version` LowCardinality(String) COMMENT 'Consensus client version of the Xatu node at the latest observation' CODEC(ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Consensus client implementation of the Xatu node at the latest observation' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `last_seen_date_time`
) ORDER BY
    (`meta_client_name`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Every Xatu node that has reported a block, with its latest identity, client and location, from fct_block_first_seen_by_node. The row with the latest last_seen_date_time wins, so a node reported in a range ending now has last_seen_date_time inside it';

CREATE TABLE fct_node_last_seen ON CLUSTER '{cluster}' AS fct_node_last_seen_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_node_last_seen_local,
    cityHash64(`meta_client_name`)
);
