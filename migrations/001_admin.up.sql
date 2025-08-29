CREATE TABLE IF NOT EXISTS `${NETWORK_NAME}`.admin_cbt_local ON CLUSTER '{cluster}' (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    position UInt64 COMMENT 'The starting position of the processed interval',
    interval UInt64 COMMENT 'The size of the interval processed',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/{database}/tables/{table}/{shard}',
    '{replica}',
    updated_date_time
)
ORDER BY (database, table, position);

CREATE TABLE IF NOT EXISTS `${NETWORK_NAME}`.admin_cbt ON CLUSTER '{cluster}' AS `${NETWORK_NAME}`.admin_cbt_local
ENGINE = Distributed(
    '{cluster}',
    '${NETWORK_NAME}',
    'admin_cbt_local',
    cityHash64(database, table)
);
