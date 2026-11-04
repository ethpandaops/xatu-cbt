-- Gloas blocks take their blob count from the blob KZG commitments in their execution payload bid.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE int_block_blob_count_canonical_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of every canonical block that carries blobs, from its execution payload blob gas (blob gas used over 131072) before Gloas, from the blob KZG commitments in its execution payload bid from Gloas, and from its blob sidecars only when neither is available';

ALTER TABLE int_block_blob_count_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block, from its execution payload blob gas before Gloas, its execution payload bid blob KZG commitments from Gloas, or its blob sidecars when neither is available';

ALTER TABLE int_block_blob_count_canonical ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of every canonical block that carries blobs, from its execution payload blob gas (blob gas used over 131072) before Gloas, from the blob KZG commitments in its execution payload bid from Gloas, and from its blob sidecars only when neither is available';

ALTER TABLE int_block_blob_count_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block, from its execution payload blob gas before Gloas, its execution payload bid blob KZG commitments from Gloas, or its blob sidecars when neither is available';
