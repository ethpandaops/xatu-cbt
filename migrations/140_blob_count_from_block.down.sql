-- Restore the comments as created in 008_block_blob_count.up.sql.

ALTER TABLE int_block_blob_count_canonical_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of a block for the finalized chain';

ALTER TABLE int_block_blob_count_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block';

ALTER TABLE int_block_blob_count_canonical ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of a block for the finalized chain';

ALTER TABLE int_block_blob_count_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block';

ALTER TABLE fct_block_blob_count_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of a block for the finalized chain';

ALTER TABLE fct_block_blob_count_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block';

ALTER TABLE fct_block_blob_count_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `status` 'Can be "canonical" or "orphaned"';

ALTER TABLE fct_block_blob_count ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of a block for the finalized chain';

ALTER TABLE fct_block_blob_count ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block';

ALTER TABLE fct_block_blob_count ON CLUSTER '{cluster}'
    COMMENT COLUMN `status` 'Can be "canonical" or "orphaned"';
