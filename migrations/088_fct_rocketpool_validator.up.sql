-- ============================================================================
-- Migration 088: Rocket Pool validators
-- ============================================================================
-- Adds:
--   1. fct_rocketpool_validator - beacon chain validators operated via Rocket
--      Pool, covering both classic minipool validators and Saturn megapool
--      validators, linked to their node operator.
-- ============================================================================

CREATE TABLE fct_rocketpool_validator_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `validator_index` UInt32 COMMENT 'The beacon chain validator index' CODEC(DoubleDelta, ZSTD(1)),
    `pubkey` String COMMENT 'The validator BLS public key, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `node_operator` String COMMENT 'The Rocket Pool node operator address, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `pool_type` LowCardinality(String) COMMENT 'The Rocket Pool staking mechanism backing this validator: minipool or megapool',
    `pool_address` String COMMENT 'The minipool contract address (minipool) or the validator withdrawal credential address (megapool), lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `created_date_time` DateTime COMMENT 'Wall clock time the backing minipool was created or the megapool deposit was made' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(created_date_time)
ORDER BY (validator_index)
COMMENT 'Beacon chain validators operated via Rocket Pool (minipool and megapool), linked to their node operator';

CREATE TABLE fct_rocketpool_validator ON CLUSTER '{cluster}' AS fct_rocketpool_validator_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_rocketpool_validator_local,
    cityHash64(validator_index)
);
