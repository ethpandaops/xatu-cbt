ALTER TABLE dim_client_release_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `name` String COMMENT 'Title of the GitHub release, empty when the tag has no release' CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS `html_url` String COMMENT 'GitHub release page, empty when the tag has no release' CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS `is_latest` Bool COMMENT 'Whether this is the latest release of the client: the release GitHub marks as latest for the repository, else the most recently published stable release' CODEC(ZSTD(1));

ALTER TABLE dim_client_release ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `name` String COMMENT 'Title of the GitHub release, empty when the tag has no release',
    ADD COLUMN IF NOT EXISTS `html_url` String COMMENT 'GitHub release page, empty when the tag has no release',
    ADD COLUMN IF NOT EXISTS `is_latest` Bool COMMENT 'Whether this is the latest release of the client: the release GitHub marks as latest for the repository, else the most recently published stable release';
