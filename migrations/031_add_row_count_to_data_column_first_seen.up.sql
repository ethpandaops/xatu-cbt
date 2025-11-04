-- Add row_count column to fct_block_data_column_sidecar_first_seen_by_node table

-- Add to local table (positioned after column_index)
ALTER TABLE `${NETWORK_NAME}`.fct_block_data_column_sidecar_first_seen_by_node_local ON CLUSTER '{cluster}'
ADD COLUMN IF NOT EXISTS `row_count` UInt32 COMMENT 'The number of rows (blobs) in the data column sidecar' CODEC(DoubleDelta, ZSTD(1)) AFTER `column_index`;

-- Add to distributed table (positioned after column_index)
ALTER TABLE `${NETWORK_NAME}`.fct_block_data_column_sidecar_first_seen_by_node ON CLUSTER '{cluster}'
ADD COLUMN IF NOT EXISTS `row_count` UInt32 COMMENT 'The number of rows (blobs) in the data column sidecar' AFTER `column_index`;
