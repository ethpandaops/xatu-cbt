-- Gloas (ePBS): self-built payloads carry builder_index = BUILDER_INDEX_SELF_BUILD
-- (UINT64_MAX), which xatu stores as NULL. The refined builder_index columns
-- were non-Nullable, so every self-build landed as builder 0 - a real builder.
-- Make them Nullable to match raw, and correct the comments: since
-- consensus-specs v1.7 builders live in their own registry, so the index is
-- neither a validator index nor equal to the proposer index on self-builds.
--
-- The two highest-value bid tables only read the gossiped bid stream, which
-- rejects self-build bids (builder_index must be a registered builder), so they
-- keep their non-Nullable type (builder_index is in the by_builder sorting and
-- sharding key) and only get their comments corrected.
--
-- Comments live on both the _local and the Distributed table, so both are
-- updated.

ALTER TABLE fct_block_payload_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` Nullable(UInt64) COMMENT 'Index of the builder in the builder registry that committed to the payload, NULL for proposer self-builds (BUILDER_INDEX_SELF_BUILD)' CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE fct_block_payload ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` Nullable(UInt64) COMMENT 'Index of the builder in the builder registry that committed to the payload, NULL for proposer self-builds (BUILDER_INDEX_SELF_BUILD)';

ALTER TABLE fct_block_payload_bid_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` Nullable(UInt64) COMMENT 'Index of the builder in the builder registry that produced the bid, NULL for proposer self-builds (BUILDER_INDEX_SELF_BUILD)' CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE fct_block_payload_bid ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` Nullable(UInt64) COMMENT 'Index of the builder in the builder registry that produced the bid, NULL for proposer self-builds (BUILDER_INDEX_SELF_BUILD)';

ALTER TABLE fct_block_payload_first_seen_by_node_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` Nullable(UInt64) COMMENT 'Index of the builder in the builder registry that produced the payload, NULL for proposer self-builds (BUILDER_INDEX_SELF_BUILD)' CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE fct_block_payload_first_seen_by_node ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` Nullable(UInt64) COMMENT 'Index of the builder in the builder registry that produced the payload, NULL for proposer self-builds (BUILDER_INDEX_SELF_BUILD)';

ALTER TABLE fct_payload_bid_highest_value_by_builder_chunked_50ms_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `builder_index` 'Index of the builder in the builder registry that produced the bid. Self-build bids are never gossiped, so this is always a registry builder';

ALTER TABLE fct_payload_bid_highest_value_by_builder_chunked_50ms ON CLUSTER '{cluster}'
    COMMENT COLUMN `builder_index` 'Index of the builder in the builder registry that produced the bid. Self-build bids are never gossiped, so this is always a registry builder';

ALTER TABLE fct_payload_bid_highest_value_chunked_50ms_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `builder_index` 'Index of the builder in the builder registry leading this chunk. Self-build bids are never gossiped, so this is always a registry builder';

ALTER TABLE fct_payload_bid_highest_value_chunked_50ms ON CLUSTER '{cluster}'
    COMMENT COLUMN `builder_index` 'Index of the builder in the builder registry leading this chunk. Self-build bids are never gossiped, so this is always a registry builder';
