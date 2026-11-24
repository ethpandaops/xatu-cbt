ALTER TABLE dim_client_release ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `repository`;

ALTER TABLE dim_client_release_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `repository`;
