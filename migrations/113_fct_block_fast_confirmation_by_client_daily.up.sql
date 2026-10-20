CREATE TABLE fct_block_fast_confirmation_by_client_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'Start of the day period' CODEC(DoubleDelta, ZSTD(1)),
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Consensus client implementation running the fast confirmation rule',
    `node_count` UInt32 COMMENT 'Number of fast confirmation nodes running this consensus client' CODEC(ZSTD(1)),
    `slot_count` UInt32 COMMENT 'Number of canonical blocks scored for this consensus client' CODEC(ZSTD(1)),
    `observation_count` UInt32 COMMENT 'Number of (canonical block, node) observations' CODEC(ZSTD(1)),
    `direct_count` UInt32 COMMENT 'Observations where the node emitted a fast confirmation event for the block itself' CODEC(ZSTD(1)),
    `descendant_count` UInt32 COMMENT 'Observations where the block was only confirmed through an event for a later block' CODEC(ZSTD(1)),
    `unconfirmed_count` UInt32 COMMENT 'Observations where the node did not fast confirm the block within 30 minutes' CODEC(ZSTD(1)),
    `orphaned_count` UInt32 COMMENT 'Fast confirmation events for blocks that did not become canonical' CODEC(ZSTD(1)),
    `avg_fast_confirmation_ms` UInt32 COMMENT 'Average milliseconds from slot start to fast confirmation' CODEC(ZSTD(1)),
    `min_fast_confirmation_ms` UInt32 COMMENT 'Minimum milliseconds from slot start to fast confirmation' CODEC(ZSTD(1)),
    `p50_fast_confirmation_ms` UInt32 COMMENT 'Median milliseconds from slot start to fast confirmation' CODEC(ZSTD(1)),
    `p90_fast_confirmation_ms` UInt32 COMMENT '90th percentile milliseconds from slot start to fast confirmation' CODEC(ZSTD(1)),
    `p95_fast_confirmation_ms` UInt32 COMMENT '95th percentile milliseconds from slot start to fast confirmation' CODEC(ZSTD(1)),
    `p99_fast_confirmation_ms` UInt32 COMMENT '99th percentile milliseconds from slot start to fast confirmation' CODEC(ZSTD(1)),
    `max_fast_confirmation_ms` UInt32 COMMENT 'Maximum milliseconds from slot start to fast confirmation' CODEC(ZSTD(1)),
    `avg_finality_ms` UInt32 COMMENT 'Average milliseconds from slot start to finality' CODEC(ZSTD(1)),
    `min_finality_ms` UInt32 COMMENT 'Minimum milliseconds from slot start to finality' CODEC(ZSTD(1)),
    `p50_finality_ms` UInt32 COMMENT 'Median milliseconds from slot start to finality' CODEC(ZSTD(1)),
    `p95_finality_ms` UInt32 COMMENT '95th percentile milliseconds from slot start to finality' CODEC(ZSTD(1)),
    `max_finality_ms` UInt32 COMMENT 'Maximum milliseconds from slot start to finality' CODEC(ZSTD(1)),
    `p50_speedup` Float32 COMMENT 'Median per-block ratio of time to finality over time to fast confirmation' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toYYYYMM(day_start_date)
ORDER BY (`day_start_date`, `meta_consensus_implementation`)
COMMENT 'Daily fast confirmation latency, reliability and speedup over finality by consensus client.';

CREATE TABLE fct_block_fast_confirmation_by_client_daily ON CLUSTER '{cluster}' AS fct_block_fast_confirmation_by_client_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_fast_confirmation_by_client_daily_local,
    cityHash64(`day_start_date`, `meta_consensus_implementation`)
);
