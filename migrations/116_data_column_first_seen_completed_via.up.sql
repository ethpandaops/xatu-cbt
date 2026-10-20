ALTER TABLE fct_block_data_column_sidecar_first_seen_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `completed_via` LowCardinality(String) COMMENT 'How the node that first saw the data column completed it: FULL_GOSSIP for a full gossip message, PARTIAL_GOSSIP, EL, RECONSTRUCTION or UNKNOWN from a column completion event, empty for beacon API events' CODEC(ZSTD(1));

ALTER TABLE fct_block_data_column_sidecar_first_seen ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `completed_via` LowCardinality(String) COMMENT 'How the node that first saw the data column completed it: FULL_GOSSIP for a full gossip message, PARTIAL_GOSSIP, EL, RECONSTRUCTION or UNKNOWN from a column completion event, empty for beacon API events';

ALTER TABLE fct_block_data_column_sidecar_first_seen_by_node_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `completed_via` LowCardinality(String) COMMENT 'How the node completed the data column: FULL_GOSSIP for a full gossip message, PARTIAL_GOSSIP, EL, RECONSTRUCTION or UNKNOWN from a column completion event, empty for beacon API events' CODEC(ZSTD(1));

ALTER TABLE fct_block_data_column_sidecar_first_seen_by_node ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `completed_via` LowCardinality(String) COMMENT 'How the node completed the data column: FULL_GOSSIP for a full gossip message, PARTIAL_GOSSIP, EL, RECONSTRUCTION or UNKNOWN from a column completion event, empty for beacon API events';
