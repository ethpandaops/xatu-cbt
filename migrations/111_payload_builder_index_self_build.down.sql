-- Revert 111: restore non-Nullable builder_index (NULL self-builds become 0 again)
-- and the original comments.

ALTER TABLE fct_block_payload ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` UInt64 DEFAULT 0 COMMENT 'Validator index of the builder, equal to the block proposer index for self-built payloads';

ALTER TABLE fct_block_payload ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` REMOVE DEFAULT;

ALTER TABLE fct_block_payload_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` UInt64 DEFAULT 0 COMMENT 'Validator index of the builder, equal to the block proposer index for self-built payloads' CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE fct_block_payload_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` REMOVE DEFAULT;

ALTER TABLE fct_block_payload_bid ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` UInt64 DEFAULT 0 COMMENT 'Validator index of the builder, equal to the block proposer index for self-built payloads';

ALTER TABLE fct_block_payload_bid ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` REMOVE DEFAULT;

ALTER TABLE fct_block_payload_bid_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` UInt64 DEFAULT 0 COMMENT 'Validator index of the builder, equal to the block proposer index for self-built payloads' CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE fct_block_payload_bid_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` REMOVE DEFAULT;

ALTER TABLE fct_block_payload_first_seen_by_node ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` UInt64 DEFAULT 0 COMMENT 'Index of the builder that produced the payload';

ALTER TABLE fct_block_payload_first_seen_by_node ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` REMOVE DEFAULT;

ALTER TABLE fct_block_payload_first_seen_by_node_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` UInt64 DEFAULT 0 COMMENT 'Index of the builder that produced the payload' CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE fct_block_payload_first_seen_by_node_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `builder_index` REMOVE DEFAULT;

ALTER TABLE fct_payload_bid_highest_value_by_builder_chunked_50ms_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `builder_index` 'Validator index of the builder that produced the bid';

ALTER TABLE fct_payload_bid_highest_value_by_builder_chunked_50ms ON CLUSTER '{cluster}'
    COMMENT COLUMN `builder_index` 'Validator index of the builder that produced the bid';

ALTER TABLE fct_payload_bid_highest_value_chunked_50ms_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `builder_index` 'Validator index of the builder leading this chunk';

ALTER TABLE fct_payload_bid_highest_value_chunked_50ms ON CLUSTER '{cluster}'
    COMMENT COLUMN `builder_index` 'Validator index of the builder leading this chunk';
