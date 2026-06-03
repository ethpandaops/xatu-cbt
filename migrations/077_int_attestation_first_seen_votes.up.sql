ALTER TABLE `${NETWORK_NAME}`.int_attestation_first_seen_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `source_epoch` UInt32 COMMENT 'Source checkpoint epoch of the attestation' CODEC(DoubleDelta, ZSTD(1)),
    ADD COLUMN IF NOT EXISTS `source_root` String COMMENT 'Source checkpoint root of the attestation' CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS `target_epoch` UInt32 COMMENT 'Target checkpoint epoch of the attestation' CODEC(DoubleDelta, ZSTD(1)),
    ADD COLUMN IF NOT EXISTS `target_root` String COMMENT 'Target checkpoint root of the attestation' CODEC(ZSTD(1));

ALTER TABLE `${NETWORK_NAME}`.int_attestation_first_seen ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `source_epoch` UInt32 COMMENT 'Source checkpoint epoch of the attestation',
    ADD COLUMN IF NOT EXISTS `source_root` String COMMENT 'Source checkpoint root of the attestation',
    ADD COLUMN IF NOT EXISTS `target_epoch` UInt32 COMMENT 'Target checkpoint epoch of the attestation',
    ADD COLUMN IF NOT EXISTS `target_root` String COMMENT 'Target checkpoint root of the attestation';
