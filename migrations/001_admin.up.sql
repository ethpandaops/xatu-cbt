CREATE TABLE IF NOT EXISTS ${NETWORK_NAME}.admin_cbt_incremental_local ON CLUSTER '{cluster}' (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    position UInt64 COMMENT 'The starting position of the processed interval',
    interval UInt64 COMMENT 'The size of the interval processed',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    updated_date_time
)
ORDER BY (database, table, position, interval);

CREATE TABLE IF NOT EXISTS ${NETWORK_NAME}.admin_cbt_incremental ON CLUSTER '{cluster}' AS ${NETWORK_NAME}.admin_cbt_incremental_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    'admin_cbt_incremental_local',
    cityHash64(database, table)
);

CREATE TABLE IF NOT EXISTS ${NETWORK_NAME}.admin_cbt_scheduled_local ON CLUSTER '{cluster}' (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    start_date_time DateTime(3) COMMENT 'The start time of the scheduled job',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    updated_date_time
)
ORDER BY (database, table);

CREATE TABLE IF NOT EXISTS ${NETWORK_NAME}.admin_cbt_scheduled ON CLUSTER '{cluster}' AS ${NETWORK_NAME}.admin_cbt_scheduled_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    'admin_cbt_scheduled_local',
    cityHash64(database, table)
);
