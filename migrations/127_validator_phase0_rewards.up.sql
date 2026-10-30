-- Phase0 attestation rewards have an inclusion delay component (included in total).
ALTER TABLE fct_attestation_reward_by_validator_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay reward in Gwei (phase0 only, the attester''s share, 0 from Altair)' CODEC(ZSTD(1)) AFTER `source`;

ALTER TABLE fct_attestation_reward_by_validator ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay reward in Gwei (phase0 only, the attester''s share, 0 from Altair)' AFTER `source`;

ALTER TABLE fct_attestation_reward_by_validator_daily_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay reward in Gwei (phase0 only, 0 from Altair)' CODEC(ZSTD(1)) AFTER `source`;

ALTER TABLE fct_attestation_reward_by_validator_daily ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay reward in Gwei (phase0 only, 0 from Altair)' AFTER `source`;

ALTER TABLE fct_attestation_reward_by_validator_weekly_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay reward in Gwei (phase0 only, 0 from Altair)' CODEC(ZSTD(1)) AFTER `source`;

ALTER TABLE fct_attestation_reward_by_validator_weekly ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay reward in Gwei (phase0 only, 0 from Altair)' AFTER `source`;

ALTER TABLE fct_validator_reward_total_daily_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `attestation_inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay rewards in Gwei (phase0 only, 0 from Altair)' CODEC(ZSTD(1)) AFTER `attestation_source`;

ALTER TABLE fct_validator_reward_total_daily ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `attestation_inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay rewards in Gwei (phase0 only, 0 from Altair)' AFTER `attestation_source`;

ALTER TABLE fct_validator_reward_total_weekly_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `attestation_inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay rewards in Gwei (phase0 only, 0 from Altair)' CODEC(ZSTD(1)) AFTER `attestation_source`;

ALTER TABLE fct_validator_reward_total_weekly ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `attestation_inclusion_delay` Int64 DEFAULT 0 COMMENT 'Inclusion delay rewards in Gwei (phase0 only, 0 from Altair)' AFTER `attestation_source`;

ALTER TABLE fct_attestation_reward_by_validator_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `total` 'head + target + source + inclusion_delay + inactivity in Gwei';

ALTER TABLE fct_attestation_reward_by_validator ON CLUSTER '{cluster}'
    COMMENT COLUMN `total` 'head + target + source + inclusion_delay + inactivity in Gwei';

ALTER TABLE fct_attestation_reward_by_validator_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `total` 'head + target + source + inclusion_delay + inactivity in Gwei';

ALTER TABLE fct_attestation_reward_by_validator_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `total` 'head + target + source + inclusion_delay + inactivity in Gwei';

ALTER TABLE fct_attestation_reward_by_validator_weekly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `total` 'head + target + source + inclusion_delay + inactivity in Gwei';

ALTER TABLE fct_attestation_reward_by_validator_weekly ON CLUSTER '{cluster}'
    COMMENT COLUMN `total` 'head + target + source + inclusion_delay + inactivity in Gwei';
