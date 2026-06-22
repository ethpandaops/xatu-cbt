-- ============================================================================
-- Migration 085: Rocket Pool Saturn megapool validators
-- ============================================================================
-- Adds:
--   1. int_rocketpool_megapool_validator - Saturn megapool validators, linked to
--      their node operator. Megapool validators do not use a per-node minipool
--      contract, so they are identified by their beacon deposit (DepositEvent)
--      sharing a transaction with a RocketNodeDeposit event; the node operator is
--      the depositor on that RocketNodeDeposit event.
-- ============================================================================

CREATE TABLE int_rocketpool_megapool_validator_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `validator_pubkey` String COMMENT 'The validator BLS public key, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `node_operator` String COMMENT 'The Rocket Pool node operator address that made the deposit, lowercase 0x-prefixed' CODEC(ZSTD(1)),
    `deposit_block_number` UInt64 COMMENT 'Execution block number of the deposit' CODEC(DoubleDelta, ZSTD(1)),
    `deposit_date_time` DateTime COMMENT 'Wall clock time of the node deposit, decoded from the RocketNodeDeposit event payload' CODEC(DoubleDelta, ZSTD(1)),
    `transaction_hash` FixedString(66) COMMENT 'Execution transaction hash of the deposit' CODEC(ZSTD(1)),
    `log_index` UInt32 COMMENT 'Log index of the beacon DepositEvent within the block' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(deposit_date_time)
ORDER BY (validator_pubkey)
COMMENT 'Rocket Pool Saturn megapool validators linked to their node operator via the deposit transaction';

CREATE TABLE int_rocketpool_megapool_validator ON CLUSTER '{cluster}' AS int_rocketpool_megapool_validator_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_rocketpool_megapool_validator_local,
    cityHash64(validator_pubkey)
);
