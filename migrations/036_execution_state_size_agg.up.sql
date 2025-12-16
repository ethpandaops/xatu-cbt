-- Hourly absolute values (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `accounts` UInt64 COMMENT 'Total accounts at end of hour' CODEC(ZSTD(1)),
    `account_bytes` UInt64 COMMENT 'Account bytes at end of hour' CODEC(ZSTD(1)),
    `account_trienodes` UInt64 COMMENT 'Account trie nodes at end of hour' CODEC(ZSTD(1)),
    `account_trienode_bytes` UInt64 COMMENT 'Account trie node bytes at end of hour' CODEC(ZSTD(1)),
    `contract_codes` UInt64 COMMENT 'Contract codes at end of hour' CODEC(ZSTD(1)),
    `contract_code_bytes` UInt64 COMMENT 'Contract code bytes at end of hour' CODEC(ZSTD(1)),
    `storages` UInt64 COMMENT 'Storage slots at end of hour' CODEC(ZSTD(1)),
    `storage_bytes` UInt64 COMMENT 'Storage bytes at end of hour' CODEC(ZSTD(1)),
    `storage_trienodes` UInt64 COMMENT 'Storage trie nodes at end of hour' CODEC(ZSTD(1)),
    `storage_trienode_bytes` UInt64 COMMENT 'Storage trie node bytes at end of hour' CODEC(ZSTD(1)),
    `total_bytes` UInt64 COMMENT 'Total state size in bytes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY (`hour_start_date_time`)
COMMENT 'Execution layer state size metrics aggregated by hour';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_hourly ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_state_size_hourly_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_state_size_hourly_local,
    cityHash64(`hour_start_date_time`)
);

-- Daily absolute values (incremental)
CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `accounts` UInt64 COMMENT 'Total accounts at end of day' CODEC(ZSTD(1)),
    `account_bytes` UInt64 COMMENT 'Account bytes at end of day' CODEC(ZSTD(1)),
    `account_trienodes` UInt64 COMMENT 'Account trie nodes at end of day' CODEC(ZSTD(1)),
    `account_trienode_bytes` UInt64 COMMENT 'Account trie node bytes at end of day' CODEC(ZSTD(1)),
    `contract_codes` UInt64 COMMENT 'Contract codes at end of day' CODEC(ZSTD(1)),
    `contract_code_bytes` UInt64 COMMENT 'Contract code bytes at end of day' CODEC(ZSTD(1)),
    `storages` UInt64 COMMENT 'Storage slots at end of day' CODEC(ZSTD(1)),
    `storage_bytes` UInt64 COMMENT 'Storage bytes at end of day' CODEC(ZSTD(1)),
    `storage_trienodes` UInt64 COMMENT 'Storage trie nodes at end of day' CODEC(ZSTD(1)),
    `storage_trienode_bytes` UInt64 COMMENT 'Storage trie node bytes at end of day' CODEC(ZSTD(1)),
    `total_bytes` UInt64 COMMENT 'Total state size in bytes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (`day_start_date`)
COMMENT 'Execution layer state size metrics aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_daily ON CLUSTER '{cluster}'
AS `${NETWORK_NAME}`.fct_execution_state_size_daily_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_state_size_daily_local,
    cityHash64(`day_start_date`)
);
