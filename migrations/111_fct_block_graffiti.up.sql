CREATE TABLE fct_block_graffiti_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number of the block' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The root of the canonical beacon block' CODEC(ZSTD(1)),
    `proposer_index` UInt32 COMMENT 'The validator index of the block proposer' CODEC(ZSTD(1)),
    `graffiti_text` String COMMENT 'The block graffiti decoded as UTF-8 with trailing null bytes removed' CODEC(ZSTD(1)),
    `graffiti_format` LowCardinality(String) COMMENT 'Which graffiti format identified the clients: codes_full, codes_long, codes_short or codes_min (client version codes with 8, 4, 2 or no commit characters), version_string (a client default such as Lighthouse/v8.2.3-6db6ae9), none (unrecognised text) or empty' CODEC(ZSTD(1)),
    `consensus_client` LowCardinality(String) COMMENT 'Consensus client named by the graffiti, e.g. lighthouse, or unknown' CODEC(ZSTD(1)),
    `consensus_client_version` LowCardinality(String) COMMENT 'Consensus client version from a version_string graffiti, e.g. v8.2.3, empty when the graffiti carries none' CODEC(ZSTD(1)),
    `consensus_client_commit` LowCardinality(String) COMMENT 'Consensus client commit prefix carried by the graffiti, empty when the graffiti carries none' CODEC(ZSTD(1)),
    `execution_client` LowCardinality(String) COMMENT 'Execution client named by the graffiti, e.g. geth, or unknown' CODEC(ZSTD(1)),
    `execution_client_commit` LowCardinality(String) COMMENT 'Execution client commit prefix carried by the graffiti, empty when the graffiti carries none' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `block_root`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Canonical beacon block graffiti with the consensus and execution clients it names, parsed from the engine API client version codes or client default graffiti';

CREATE TABLE fct_block_graffiti ON CLUSTER '{cluster}' AS fct_block_graffiti_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_local,
    cityHash64(`slot_start_date_time`, `block_root`)
);

ALTER TABLE fct_block_graffiti_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `block_root`)
);

CREATE TABLE fct_block_graffiti_client_hourly_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The wall clock time at the start of the hour' CODEC(DoubleDelta, ZSTD(1)),
    `consensus_client` LowCardinality(String) COMMENT 'Consensus client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `execution_client` LowCardinality(String) COMMENT 'Execution client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks in the hour with this consensus and execution client pair' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `consensus_client`, `execution_client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Hourly canonical block counts per consensus and execution client pair named by block graffiti, including unknown';

CREATE TABLE fct_block_graffiti_client_hourly ON CLUSTER '{cluster}' AS fct_block_graffiti_client_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_hourly_local,
    cityHash64(`hour_start_date_time`, `consensus_client`, `execution_client`)
);

CREATE TABLE fct_block_graffiti_client_daily_local on cluster '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `consensus_client` LowCardinality(String) COMMENT 'Consensus client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `execution_client` LowCardinality(String) COMMENT 'Execution client named by the block graffiti, or unknown' CODEC(ZSTD(1)),
    `block_count` UInt32 COMMENT 'Number of canonical blocks in the day with this consensus and execution client pair' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY
    (`day_start_date`, `consensus_client`, `execution_client`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Daily canonical block counts per consensus and execution client pair named by block graffiti, including unknown';

CREATE TABLE fct_block_graffiti_client_daily ON CLUSTER '{cluster}' AS fct_block_graffiti_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_graffiti_client_daily_local,
    cityHash64(`day_start_date`, `consensus_client`, `execution_client`)
);
