CREATE TABLE fct_block_fast_confirmation_by_node_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number of the block' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `block_root` String COMMENT 'The beacon block root hash' CODEC(ZSTD(1)),
    `status` LowCardinality(String) COMMENT 'Chain status of the block: canonical, or orphaned when a node fast confirmed a block that did not become canonical',
    `meta_client_name` LowCardinality(String) COMMENT 'Name of the sentry node that emitted the fast confirmation events',
    `meta_consensus_implementation` LowCardinality(String) COMMENT 'Consensus client implementation running the fast confirmation rule',
    `meta_consensus_version` LowCardinality(String) COMMENT 'Consensus client version running the fast confirmation rule',
    `confirmation_type` LowCardinality(String) COMMENT 'How the block was fast confirmed: direct (an event for this block), descendant (only implied by an event for a later block), or unconfirmed (no confirmation seen within 30 minutes)',
    `fast_confirmed_slot_start_diff` Nullable(UInt32) COMMENT 'Milliseconds from slot start until the node fast confirmed this block, directly or through a descendant' CODEC(ZSTD(1)),
    `fast_confirmation_slot` Nullable(UInt32) COMMENT 'Slot of the block whose fast confirmation event first covered this block. Equals slot for direct confirmations' CODEC(ZSTD(1)),
    `finalized_slot_start_diff` Nullable(UInt32) COMMENT 'Milliseconds from slot start until the first sentry observed a finalized checkpoint covering this block' CODEC(ZSTD(1)),
    `finalized_epoch` Nullable(UInt32) COMMENT 'Epoch of the finalized checkpoint that first covered this block' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY (`slot_start_date_time`, `meta_client_name`, `block_root`)
COMMENT 'One row per (block, fast confirmation node) with the time the node fast confirmed the block and the time the block was finalized.';

CREATE TABLE fct_block_fast_confirmation_by_node ON CLUSTER '{cluster}' AS fct_block_fast_confirmation_by_node_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_block_fast_confirmation_by_node_local,
    cityHash64(`slot_start_date_time`, `meta_client_name`, `block_root`)
);
