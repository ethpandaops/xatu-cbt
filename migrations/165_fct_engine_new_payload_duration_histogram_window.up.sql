CREATE TABLE fct_engine_new_payload_duration_histogram_window_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `window_name` LowCardinality(String) COMMENT 'The window: 1h, 6h, 24h, 7d or 31d ending at the last complete hour, or 90d, 180d, 1y or all ending at the end of yesterday (UTC)',
    `window_start_date_time` DateTime COMMENT 'Start of the window, inclusive' CODEC(DoubleDelta, ZSTD(1)),
    `window_end_date_time` DateTime COMMENT 'End of the window, exclusive' CODEC(DoubleDelta, ZSTD(1)),
    `node_class` LowCardinality(String) COMMENT 'Node classification (eip7870-block-builder for reference nodes, empty for other Xatu nodes)',
    `meta_execution_implementation` LowCardinality(String) COMMENT 'Execution client implementation name (e.g., Geth, Nethermind, Besu, Reth)',
    `meta_execution_version` LowCardinality(String) COMMENT 'Full execution client version string from web3_clientVersion RPC',
    `status` LowCardinality(String) COMMENT 'Engine API response status (VALID, INVALID, SYNCING, ACCEPTED, INVALID_BLOCK_HASH, ERROR)',
    `bucket_index` UInt16 COMMENT 'Duration bucket: 0 to 24 are 1ms wide, from 25 each bucket is about 4% wider than the last' CODEC(ZSTD(1)),
    `bucket_lower_ms` UInt32 COMMENT 'Lower bound of the bucket in milliseconds, inclusive' CODEC(ZSTD(1)),
    `bucket_upper_ms` UInt32 COMMENT 'Upper bound of the bucket in milliseconds, exclusive' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of engine_newPayload calls in the window whose duration falls in the bucket, 0 for a key that left the window' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
)
ORDER BY (`window_name`, `node_class`, `meta_execution_implementation`, `meta_execution_version`, `status`, `bucket_index`)
COMMENT 'engine_newPayload duration histograms per window (1h to all, aligned like Lab ranges), node class, execution client, version and status, recomputed hourly, for true range percentiles per client and version';

CREATE TABLE fct_engine_new_payload_duration_histogram_window ON CLUSTER '{cluster}' AS fct_engine_new_payload_duration_histogram_window_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_engine_new_payload_duration_histogram_window_local,
    cityHash64(`window_name`, `meta_execution_implementation`)
);
