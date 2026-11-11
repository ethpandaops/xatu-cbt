ALTER TABLE dim_client_release_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `repository` LowCardinality(String) COMMENT 'GitHub repository the release tags come from, e.g. sigp/lighthouse' CODEC(ZSTD(1));

ALTER TABLE dim_client_release ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `repository` LowCardinality(String) COMMENT 'GitHub repository the release tags come from, e.g. sigp/lighthouse';
