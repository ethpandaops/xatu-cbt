ALTER TABLE `${NETWORK_NAME}`.int_attestation_first_seen ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `source_epoch`,
    DROP COLUMN IF EXISTS `source_root`,
    DROP COLUMN IF EXISTS `target_epoch`,
    DROP COLUMN IF EXISTS `target_root`;

ALTER TABLE `${NETWORK_NAME}`.int_attestation_first_seen_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `source_epoch`,
    DROP COLUMN IF EXISTS `source_root`,
    DROP COLUMN IF EXISTS `target_epoch`,
    DROP COLUMN IF EXISTS `target_root`;
