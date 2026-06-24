-- ============================================================================
-- Migration 087: Rocket Pool node timezone locations
-- ============================================================================
-- Adds:
--   1. int_rocketpool_node_timezone - timezone (location) strings decoded from
--      registerNode(string) and setTimezoneLocation(string) calldata sent to the
--      RocketNodeManager contract. The current timezone for a node is the row
--      with the greatest set_block_number.
-- ============================================================================

CREATE TABLE int_rocketpool_node_timezone_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `node_address` String COMMENT 'The Rocket Pool node operator address, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `timezone` String COMMENT 'The self-reported timezone location string (e.g. Europe/London)' CODEC(ZSTD(1)),
    `set_block_number` UInt64 COMMENT 'Execution block number the timezone was set in' CODEC(DoubleDelta, ZSTD(1)),
    `set_date_time` DateTime COMMENT 'Wall clock time of the block the timezone was set in' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(set_date_time)
ORDER BY (node_address, set_block_number)
COMMENT 'Rocket Pool node timezone locations decoded from registerNode/setTimezoneLocation calldata';

CREATE TABLE int_rocketpool_node_timezone ON CLUSTER '{cluster}' AS int_rocketpool_node_timezone_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_rocketpool_node_timezone_local,
    cityHash64(node_address)
);
