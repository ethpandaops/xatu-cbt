ALTER TABLE fct_attestation_reward_by_validator ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `inclusion_delay`;
ALTER TABLE fct_attestation_reward_by_validator_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `inclusion_delay`;

ALTER TABLE fct_attestation_reward_by_validator_daily ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `inclusion_delay`;
ALTER TABLE fct_attestation_reward_by_validator_daily_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `inclusion_delay`;

ALTER TABLE fct_attestation_reward_by_validator_weekly ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `inclusion_delay`;
ALTER TABLE fct_attestation_reward_by_validator_weekly_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `inclusion_delay`;

ALTER TABLE fct_validator_reward_total_daily ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `attestation_inclusion_delay`;
ALTER TABLE fct_validator_reward_total_daily_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `attestation_inclusion_delay`;

ALTER TABLE fct_validator_reward_total_weekly ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `attestation_inclusion_delay`;
ALTER TABLE fct_validator_reward_total_weekly_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `attestation_inclusion_delay`;
