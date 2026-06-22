-- ============================================================================
-- Migration 086: Rocket Pool node lifecycle events
-- ============================================================================
-- Adds:
--   1. int_rocketpool_node_event - node registration, RPL stake/withdraw and
--      smoothing pool state-change events decoded from execution layer logs.
--      Block timestamps are resolved by joining canonical_execution_block.
-- ============================================================================

CREATE TABLE int_rocketpool_node_event_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `event_name` LowCardinality(String) COMMENT 'Node event name: node_registered, rpl_staked, rpl_withdrawn or smoothing_pool_state_changed',
    `node_address` String COMMENT 'The Rocket Pool node operator address, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `rpl_amount_wei` UInt256 COMMENT 'RPL amount in wei for rpl_staked/rpl_withdrawn events, otherwise 0' CODEC(ZSTD(1)),
    `smoothing_state` Bool COMMENT 'Smoothing pool membership state for smoothing_pool_state_changed events, otherwise false',
    `event_block_number` UInt64 COMMENT 'Execution block number the event was emitted in' CODEC(DoubleDelta, ZSTD(1)),
    `event_date_time` DateTime COMMENT 'Wall clock time of the block the event was emitted in' CODEC(DoubleDelta, ZSTD(1)),
    `log_index` UInt32 COMMENT 'Log index of the event within the block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(event_date_time)
ORDER BY (event_name, node_address, event_block_number, log_index)
COMMENT 'Rocket Pool node lifecycle events (registration, RPL stake/withdraw, smoothing pool) decoded from execution layer logs';

CREATE TABLE int_rocketpool_node_event ON CLUSTER '{cluster}' AS int_rocketpool_node_event_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_rocketpool_node_event_local,
    cityHash64(event_name, node_address)
);
