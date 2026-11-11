ALTER TABLE fct_attestation_vote_correctness_by_validator_hourly ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `source_incorrect_count`;

ALTER TABLE fct_attestation_vote_correctness_by_validator_hourly_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `source_incorrect_count`;

ALTER TABLE fct_attestation_vote_correctness_by_validator_daily ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `source_incorrect_count`;

ALTER TABLE fct_attestation_vote_correctness_by_validator_daily_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `source_incorrect_count`;

ALTER TABLE fct_attestation_vote_correctness_by_validator_weekly ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `source_incorrect_count`;

ALTER TABLE fct_attestation_vote_correctness_by_validator_weekly_local ON CLUSTER '{cluster}' DROP COLUMN IF EXISTS `source_incorrect_count`;
