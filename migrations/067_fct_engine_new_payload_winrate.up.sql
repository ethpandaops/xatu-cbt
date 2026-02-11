-- fct_engine_new_payload_winrate_hourly
CREATE TABLE `${NETWORK_NAME}`.fct_engine_new_payload_winrate_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation name (e.g., Reth, Nethermind, Besu)' CODEC(ZSTD(1)),
    `win_count` UInt32 COMMENT 'Number of slots where this client had the fastest engine_newPayload duration' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (hour_start_date_time, meta_execution_implementation)
COMMENT 'Hourly execution client winrate based on fastest engine_newPayload duration per slot (7870 nodes only)';

CREATE TABLE `${NETWORK_NAME}`.fct_engine_new_payload_winrate_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_engine_new_payload_winrate_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_engine_new_payload_winrate_hourly_local,
    cityHash64(hour_start_date_time)
);

-- fct_engine_new_payload_winrate_daily
CREATE TABLE `${NETWORK_NAME}`.fct_engine_new_payload_winrate_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation name (e.g., Reth, Nethermind, Besu)' CODEC(ZSTD(1)),
    `win_count` UInt32 COMMENT 'Number of slots where this client had the fastest engine_newPayload duration' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(day_start_date)
ORDER BY (day_start_date, meta_execution_implementation)
COMMENT 'Daily execution client winrate based on fastest engine_newPayload duration per slot (7870 nodes only)';

CREATE TABLE `${NETWORK_NAME}`.fct_engine_new_payload_winrate_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_engine_new_payload_winrate_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_engine_new_payload_winrate_daily_local,
    cityHash64(day_start_date)
);
