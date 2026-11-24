-- Restore the comments set in 140_blob_count_from_block.up.sql.

ALTER TABLE int_block_blob_count_canonical_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of every canonical block that carries blobs, from its execution payload blob gas (blob gas used over 131072), or from its blob sidecars when the beacon block has no execution payload (Gloas onwards)';

ALTER TABLE int_block_blob_count_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block, from its execution payload blob gas, or its blob sidecars from Gloas onwards';

ALTER TABLE int_block_blob_count_canonical ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of every canonical block that carries blobs, from its execution payload blob gas (blob gas used over 131072), or from its blob sidecars when the beacon block has no execution payload (Gloas onwards)';

ALTER TABLE int_block_blob_count_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block, from its execution payload blob gas, or its blob sidecars from Gloas onwards';
