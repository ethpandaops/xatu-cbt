CREATE TABLE fct_node_count_by_cl_client_version_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The UTC day' CODEC(DoubleDelta, ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'The consensus client the Xatu nodes ran',
    `meta_consensus_version` LowCardinality(String) COMMENT 'The consensus client version the Xatu nodes reported, as the client reports it',
    `node_count` UInt32 COMMENT 'The number of distinct Xatu nodes whose last reported version that day was this one' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY
    (`day_start_date`, `meta_consensus_implementation`, `meta_consensus_version`)
COMMENT 'Xatu nodes per day, consensus client and consensus client version, from the heads each node saw';

CREATE TABLE fct_node_count_by_cl_client_version_daily ON CLUSTER '{cluster}' AS fct_node_count_by_cl_client_version_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_node_count_by_cl_client_version_daily_local,
    cityHash64(`day_start_date`, `meta_consensus_implementation`)
);

CREATE TABLE fct_node_count_by_cl_client_version_window_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `window_name` LowCardinality(String) COMMENT 'The window: 30d, 90d or 1y for the last 30, 90 or 365 complete UTC days, or all for all history',
    `window_start_date` Date COMMENT 'The first UTC day in the window' CODEC(DoubleDelta, ZSTD(1)),
    `window_end_date` Date COMMENT 'The last UTC day in the window, the last complete day' CODEC(DoubleDelta, ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'The consensus client the Xatu nodes ran',
    `meta_consensus_version` LowCardinality(String) COMMENT 'The consensus client version the Xatu nodes reported, as the client reports it',
    `node_count` UInt32 COMMENT 'The number of distinct Xatu nodes that reported this version on any day of the window, 0 when none did' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY
    (`window_name`, `meta_consensus_implementation`, `meta_consensus_version`)
COMMENT 'Distinct Xatu nodes per consensus client and version over the last 30, 90 and 365 complete UTC days and all history, recomputed hourly';

CREATE TABLE fct_node_count_by_cl_client_version_window ON CLUSTER '{cluster}' AS fct_node_count_by_cl_client_version_window_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_node_count_by_cl_client_version_window_local,
    cityHash64(`window_name`, `meta_consensus_implementation`)
);
