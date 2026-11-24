ALTER TABLE fct_attestation_reward_by_validator ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `total`;

ALTER TABLE fct_attestation_reward_by_validator_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `total`;

ALTER TABLE fct_attestation_reward_by_validator_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `total` Int64 COMMENT 'head + target + source + inclusion_delay + inactivity in Gwei' CODEC(ZSTD(1)) AFTER `inactivity`;

ALTER TABLE fct_attestation_reward_by_validator ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `total` Int64 COMMENT 'head + target + source + inclusion_delay + inactivity in Gwei' AFTER `inactivity`;
