CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'Start of the hour period' CODEC(DoubleDelta, ZSTD(1)),
    `measurement_count` UInt32 COMMENT 'Number of measurements in this hour' CODEC(ZSTD(1)),
    
    `min_block_number` UInt64 COMMENT 'Minimum block number in this hour' CODEC(DoubleDelta, ZSTD(1)),
    `max_block_number` UInt64 COMMENT 'Maximum block number in this hour' CODEC(DoubleDelta, ZSTD(1)),
    
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
    
    `accounts_delta` Int64 COMMENT 'Account count change within the hour' CODEC(ZSTD(1)),
    `account_bytes_delta` Int64 COMMENT 'Account bytes change within the hour' CODEC(ZSTD(1)),
    `storage_bytes_delta` Int64 COMMENT 'Storage bytes change within the hour' CODEC(ZSTD(1)),
    `contract_code_bytes_delta` Int64 COMMENT 'Contract code bytes change within the hour' CODEC(ZSTD(1)),
    
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

CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `date` Date COMMENT 'Date of the aggregation' CODEC(DoubleDelta, ZSTD(1)),
    `hour_count` UInt32 COMMENT 'Number of hours in this day' CODEC(ZSTD(1)),
    
    `min_block_number` UInt64 COMMENT 'Minimum block number in this day' CODEC(DoubleDelta, ZSTD(1)),
    `max_block_number` UInt64 COMMENT 'Maximum block number in this day' CODEC(DoubleDelta, ZSTD(1)),
    
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
    
    `accounts_delta` Int64 COMMENT 'Account count change within the day' CODEC(ZSTD(1)),
    `account_bytes_delta` Int64 COMMENT 'Account bytes change within the day' CODEC(ZSTD(1)),
    `storage_bytes_delta` Int64 COMMENT 'Storage bytes change within the day' CODEC(ZSTD(1)),
    `contract_code_bytes_delta` Int64 COMMENT 'Contract code bytes change within the day' CODEC(ZSTD(1)),
    
    `total_bytes` UInt64 COMMENT 'Total state size in bytes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(date)
ORDER BY (`date`)
COMMENT 'Execution layer state size metrics aggregated by day';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_daily ON CLUSTER '{cluster}' 
AS `${NETWORK_NAME}`.fct_execution_state_size_daily_local 
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_state_size_daily_local,
    cityHash64(`date`)
);

CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_monthly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `month` Date COMMENT 'First day of the month' CODEC(DoubleDelta, ZSTD(1)),
    `day_count` UInt32 COMMENT 'Number of days in this month with data' CODEC(ZSTD(1)),
    
    `min_block_number` UInt64 COMMENT 'Minimum block number in this month' CODEC(DoubleDelta, ZSTD(1)),
    `max_block_number` UInt64 COMMENT 'Maximum block number in this month' CODEC(DoubleDelta, ZSTD(1)),
    
    `accounts` UInt64 COMMENT 'Total accounts at end of month' CODEC(ZSTD(1)),
    `account_bytes` UInt64 COMMENT 'Account bytes at end of month' CODEC(ZSTD(1)),
    `account_trienodes` UInt64 COMMENT 'Account trie nodes at end of month' CODEC(ZSTD(1)),
    `account_trienode_bytes` UInt64 COMMENT 'Account trie node bytes at end of month' CODEC(ZSTD(1)),
    
    `contract_codes` UInt64 COMMENT 'Contract codes at end of month' CODEC(ZSTD(1)),
    `contract_code_bytes` UInt64 COMMENT 'Contract code bytes at end of month' CODEC(ZSTD(1)),
    
    `storages` UInt64 COMMENT 'Storage slots at end of month' CODEC(ZSTD(1)),
    `storage_bytes` UInt64 COMMENT 'Storage bytes at end of month' CODEC(ZSTD(1)),
    `storage_trienodes` UInt64 COMMENT 'Storage trie nodes at end of month' CODEC(ZSTD(1)),
    `storage_trienode_bytes` UInt64 COMMENT 'Storage trie node bytes at end of month' CODEC(ZSTD(1)),
    
    `accounts_delta` Int64 COMMENT 'Account count change within the month' CODEC(ZSTD(1)),
    `account_bytes_delta` Int64 COMMENT 'Account bytes change within the month' CODEC(ZSTD(1)),
    `storage_bytes_delta` Int64 COMMENT 'Storage bytes change within the month' CODEC(ZSTD(1)),
    `contract_code_bytes_delta` Int64 COMMENT 'Contract code bytes change within the month' CODEC(ZSTD(1)),
    
    `total_bytes` UInt64 COMMENT 'Total state size in bytes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYear(month)
ORDER BY (`month`)
COMMENT 'Execution layer state size metrics aggregated by month';

CREATE TABLE `${NETWORK_NAME}`.fct_execution_state_size_monthly ON CLUSTER '{cluster}' 
AS `${NETWORK_NAME}`.fct_execution_state_size_monthly_local 
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    fct_execution_state_size_monthly_local,
    cityHash64(`month`)
);
