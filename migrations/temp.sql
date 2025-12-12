CREATE TABLE `mainnet`.fct_execution_state_size_weekly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week` Date COMMENT 'First day of the week (Monday)' CODEC(DoubleDelta, ZSTD(1)),
    `accounts` UInt64 COMMENT 'Total accounts at end of week' CODEC(ZSTD(1)),
    `account_bytes` UInt64 COMMENT 'Account bytes at end of week' CODEC(ZSTD(1)),
    `account_trienodes` UInt64 COMMENT 'Account trie nodes at end of week' CODEC(ZSTD(1)),
    `account_trienode_bytes` UInt64 COMMENT 'Account trie node bytes at end of week' CODEC(ZSTD(1)),
    `contract_codes` UInt64 COMMENT 'Contract codes at end of week' CODEC(ZSTD(1)),
    `contract_code_bytes` UInt64 COMMENT 'Contract code bytes at end of week' CODEC(ZSTD(1)),
    `storages` UInt64 COMMENT 'Storage slots at end of week' CODEC(ZSTD(1)),
    `storage_bytes` UInt64 COMMENT 'Storage bytes at end of week' CODEC(ZSTD(1)),
    `storage_trienodes` UInt64 COMMENT 'Storage trie nodes at end of week' CODEC(ZSTD(1)),
    `storage_trienode_bytes` UInt64 COMMENT 'Storage trie node bytes at end of week' CODEC(ZSTD(1)),
    `total_bytes` UInt64 COMMENT 'Total state size in bytes' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(week)
ORDER BY (`week`)
COMMENT 'Execution layer state size metrics aggregated by week';

CREATE TABLE `mainnet`.fct_execution_state_size_weekly ON CLUSTER '{cluster}'
AS `mainnet`.fct_execution_state_size_weekly_local
ENGINE = Distributed(
    '{cluster}',
    'mainnet',
    fct_execution_state_size_weekly_local,
    cityHash64(`week`)
);

-- Weekly deltas (scheduled)
CREATE TABLE `mainnet`.fct_execution_state_size_weekly_delta_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `week` Date COMMENT 'First day of the week (Monday)' CODEC(DoubleDelta, ZSTD(1)),
    `account_count_delta` Int64 COMMENT 'Account count change within the week' CODEC(ZSTD(1)),
    `account_bytes_delta` Int64 COMMENT 'Account bytes change within the week' CODEC(ZSTD(1)),
    `account_trienodes_delta` Int64 COMMENT 'Account trie nodes change within the week' CODEC(ZSTD(1)),
    `account_trienode_bytes_delta` Int64 COMMENT 'Account trie node bytes change within the week' CODEC(ZSTD(1)),
    `contract_codes_delta` Int64 COMMENT 'Contract codes change within the week' CODEC(ZSTD(1)),
    `contract_code_bytes_delta` Int64 COMMENT 'Contract code bytes change within the week' CODEC(ZSTD(1)),
    `storage_count_delta` Int64 COMMENT 'Storage slots change within the week' CODEC(ZSTD(1)),
    `storage_bytes_delta` Int64 COMMENT 'Storage bytes change within the week' CODEC(ZSTD(1)),
    `storage_trienodes_delta` Int64 COMMENT 'Storage trie nodes change within the week' CODEC(ZSTD(1)),
    `storage_trienode_bytes_delta` Int64 COMMENT 'Storage trie node bytes change within the week' CODEC(ZSTD(1)),
    `total_bytes_delta` Int64 COMMENT 'Total state size change within the week' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(week)
ORDER BY (`week`)
COMMENT 'Execution layer state size deltas aggregated by week';

CREATE TABLE `mainnet`.fct_execution_state_size_weekly_delta ON CLUSTER '{cluster}'
AS `mainnet`.fct_execution_state_size_weekly_delta_local
ENGINE = Distributed(
    '{cluster}',
    'mainnet',
    fct_execution_state_size_weekly_delta_local,
    cityHash64(`week`)
);

INSERT INTO `mainnet`.fct_execution_state_size_weekly_delta
SELECT
    now() as updated_date_time,
    toStartOfWeek(b.block_date_time, 1) AS week,
    toInt64(argMax(s.accounts, s.block_number)) - toInt64(argMin(s.accounts, s.block_number)) AS account_count_delta,
    toInt64(argMax(s.account_bytes, s.block_number)) - toInt64(argMin(s.account_bytes, s.block_number)) AS account_bytes_delta,
    toInt64(argMax(s.account_trienodes, s.block_number)) - toInt64(argMin(s.account_trienodes, s.block_number)) AS account_trienodes_delta,
    toInt64(argMax(s.account_trienode_bytes, s.block_number)) - toInt64(argMin(s.account_trienode_bytes, s.block_number)) AS account_trienode_bytes_delta,
    toInt64(argMax(s.contract_codes, s.block_number)) - toInt64(argMin(s.contract_codes, s.block_number)) AS contract_codes_delta,
    toInt64(argMax(s.contract_code_bytes, s.block_number)) - toInt64(argMin(s.contract_code_bytes, s.block_number)) AS contract_code_bytes_delta,
    toInt64(argMax(s.storages, s.block_number)) - toInt64(argMin(s.storages, s.block_number)) AS storage_count_delta,
    toInt64(argMax(s.storage_bytes, s.block_number)) - toInt64(argMin(s.storage_bytes, s.block_number)) AS storage_bytes_delta,
    toInt64(argMax(s.storage_trienodes, s.block_number)) - toInt64(argMin(s.storage_trienodes, s.block_number)) AS storage_trienodes_delta,
    toInt64(argMax(s.storage_trienode_bytes, s.block_number)) - toInt64(argMin(s.storage_trienode_bytes, s.block_number)) AS storage_trienode_bytes_delta,
    (toInt64(argMax(s.account_trienode_bytes, s.block_number)) + toInt64(argMax(s.contract_code_bytes, s.block_number)) + toInt64(argMax(s.storage_trienode_bytes, s.block_number)))
        - (toInt64(argMin(s.account_trienode_bytes, s.block_number)) + toInt64(argMin(s.contract_code_bytes, s.block_number)) + toInt64(argMin(s.storage_trienode_bytes, s.block_number))) AS total_bytes_delta
FROM default.execution_state_size AS s FINAL
GLOBAL INNER JOIN default.canonical_execution_block AS b FINAL
    ON s.block_number = b.block_number
WHERE b.block_date_time >= now() - INTERVAL 52 WEEK
GROUP BY toStartOfWeek(b.block_date_time, 1);

INSERT INTO `mainnet`.fct_execution_state_size_weekly
SELECT
    now() as updated_date_time,
    toStartOfWeek(b.block_date_time, 1) AS week,
    argMax(s.accounts, s.block_number) AS accounts,
    argMax(s.account_bytes, s.block_number) AS account_bytes,
    argMax(s.account_trienodes, s.block_number) AS account_trienodes,
    argMax(s.account_trienode_bytes, s.block_number) AS account_trienode_bytes,
    argMax(s.contract_codes, s.block_number) AS contract_codes,
    argMax(s.contract_code_bytes, s.block_number) AS contract_code_bytes,
    argMax(s.storages, s.block_number) AS storages,
    argMax(s.storage_bytes, s.block_number) AS storage_bytes,
    argMax(s.storage_trienodes, s.block_number) AS storage_trienodes,
    argMax(s.storage_trienode_bytes, s.block_number) AS storage_trienode_bytes,
    argMax(s.account_trienode_bytes, s.block_number)
        + argMax(s.contract_code_bytes, s.block_number)
        + argMax(s.storage_trienode_bytes, s.block_number) AS total_bytes
FROM {{ index .dep "{{external}}" "execution_state_size" "helpers" "from" }} AS s FINAL
GLOBAL INNER JOIN {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} AS b FINAL
    ON s.block_number = b.block_number
WHERE s.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY toStartOfWeek(b.block_date_time, 1)