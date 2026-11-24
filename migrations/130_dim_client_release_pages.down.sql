ALTER TABLE dim_client_release ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `name`,
    DROP COLUMN IF EXISTS `html_url`,
    DROP COLUMN IF EXISTS `is_latest`;

ALTER TABLE dim_client_release_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `name`,
    DROP COLUMN IF EXISTS `html_url`,
    DROP COLUMN IF EXISTS `is_latest`;
