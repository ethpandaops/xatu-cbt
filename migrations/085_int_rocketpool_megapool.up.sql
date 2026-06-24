-- ============================================================================
-- Migration 085: Rocket Pool Saturn megapools
-- ============================================================================
-- Adds:
--   1. int_rocketpool_megapool - Saturn megapool contracts mapped to their node
--      operator. Megapools are deployed (CREATE2) by the RocketMegapoolFactory on
--      a node's first deposit. The node operator is the depositor on the
--      RocketNodeDeposit event in the same transaction. Megapool validators are
--      then linked in fct_rocketpool_validator by matching the validator
--      withdrawal credential to the megapool address (same as minipools).
-- ============================================================================

CREATE TABLE int_rocketpool_megapool_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `megapool_address` String COMMENT 'The Rocket Pool megapool contract address, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `node_operator` String COMMENT 'The Rocket Pool node operator that owns the megapool, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `created_block_number` UInt64 COMMENT 'Execution block number the megapool was deployed in' CODEC(DoubleDelta, ZSTD(1)),
    `created_date_time` DateTime COMMENT 'Wall clock time the megapool was deployed (decoded from the deposit event)' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'Execution transaction hash that deployed the megapool' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(created_date_time)
ORDER BY (megapool_address)
COMMENT 'Rocket Pool Saturn megapool contracts mapped to their node operator';

CREATE TABLE int_rocketpool_megapool ON CLUSTER '{cluster}' AS int_rocketpool_megapool_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_rocketpool_megapool_local,
    cityHash64(megapool_address)
);
