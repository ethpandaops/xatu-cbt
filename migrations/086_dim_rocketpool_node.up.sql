-- ============================================================================
-- Migration 086: Rocket Pool node operators
-- ============================================================================
-- Adds:
--   1. dim_rocketpool_node - Rocket Pool node operators with their minipool and
--                            validator counts.
-- ============================================================================

CREATE TABLE dim_rocketpool_node_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `node_operator` String COMMENT 'The Rocket Pool node operator address, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `minipool_count` UInt32 COMMENT 'Total number of minipools ever created by this node operator' CODEC(ZSTD(1)),
    `active_minipool_count` UInt32 COMMENT 'Number of minipools created minus destroyed for this node operator' CODEC(ZSTD(1)),
    `validator_count` UInt32 COMMENT 'Number of beacon chain validators linked to this node operator' CODEC(ZSTD(1)),
    `first_minipool_date_time` DateTime COMMENT 'Wall clock time the node operators first minipool was created' CODEC(DoubleDelta, ZSTD(1)),
    `last_minipool_date_time` DateTime COMMENT 'Wall clock time the node operators most recent minipool event occurred' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(first_minipool_date_time)
ORDER BY (node_operator)
COMMENT 'Rocket Pool node operators with their minipool and validator counts';

CREATE TABLE dim_rocketpool_node ON CLUSTER '{cluster}' AS dim_rocketpool_node_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    dim_rocketpool_node_local,
    cityHash64(node_operator)
);
