DROP TABLE IF EXISTS fct_block_proposer_head ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_block_proposer_head_local ON CLUSTER '{cluster}' SYNC;

CREATE TABLE fct_block_proposer_head_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `proposer_validator_index` UInt32 COMMENT 'The validator index of the proposer for the slot' CODEC(ZSTD(1)),
    `proposer_pubkey` String COMMENT 'The public key of the validator proposer' CODEC(ZSTD(1)),
    `block_root` Nullable(String) COMMENT 'The beacon block root hash. Null if a block was never seen by a sentry' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `proposer_validator_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild'
COMMENT 'Block proposers for the unfinalized chain';

CREATE TABLE fct_block_proposer_head ON CLUSTER '{cluster}' AS fct_block_proposer_head_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_proposer_head_local,
    cityHash64(`slot_start_date_time`, `proposer_validator_index`)
);

ALTER TABLE fct_block_proposer_head_local ON CLUSTER '{cluster}'
ADD PROJECTION p_by_slot
(
    SELECT *
    ORDER BY (`slot`, `proposer_validator_index`)
);

-- Reset CBT processing positions so both tables rebuild from source.
ALTER TABLE admin_cbt_incremental_local ON CLUSTER '{cluster}'
DELETE WHERE `table` IN (
    'fct_block_proposer_head',
    'fct_block_proposer_entity'
) SETTINGS mutations_sync = 2;
