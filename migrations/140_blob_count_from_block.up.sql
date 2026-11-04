-- Describe where canonical blob counts and the orphaned status now come from.
--
-- int_block_blob_count_canonical counts blobs from the execution payload blob gas of the block (blob gas used over
-- GAS_PER_BLOB, 131072) and only falls back to blob sidecars when the beacon block carries no execution payload
-- (Gloas onwards). fct_block_blob_count calls a block orphaned when the canonical chain holds a different block,
-- or none, at its slot, not when its sidecars are missing.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE int_block_blob_count_canonical_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of every canonical block that carries blobs, from its execution payload blob gas (blob gas used over 131072), or from its blob sidecars when the beacon block has no execution payload (Gloas onwards)';

ALTER TABLE int_block_blob_count_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block, from its execution payload blob gas, or its blob sidecars from Gloas onwards';

ALTER TABLE int_block_blob_count_canonical ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of every canonical block that carries blobs, from its execution payload blob gas (blob gas used over 131072), or from its blob sidecars when the beacon block has no execution payload (Gloas onwards)';

ALTER TABLE int_block_blob_count_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block, from its execution payload blob gas, or its blob sidecars from Gloas onwards';

ALTER TABLE fct_block_blob_count_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of every block that carries blobs, canonical blocks from int_block_blob_count_canonical and orphaned ones from the unfinalized chain';

ALTER TABLE fct_block_blob_count_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block';

ALTER TABLE fct_block_blob_count_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `status` 'canonical when the block is on the finalized chain, orphaned when the finalized chain has a different block or none at its slot';

ALTER TABLE fct_block_blob_count ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Blob count of every block that carries blobs, canonical blocks from int_block_blob_count_canonical and orphaned ones from the unfinalized chain';

ALTER TABLE fct_block_blob_count ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'The number of blobs in the block';

ALTER TABLE fct_block_blob_count ON CLUSTER '{cluster}'
    COMMENT COLUMN `status` 'canonical when the block is on the finalized chain, orphaned when the finalized chain has a different block or none at its slot';
