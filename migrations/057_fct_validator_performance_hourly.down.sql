DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_hourly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_validator_balance_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_validator_balance_hourly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_hourly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_hourly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_attestation_vote_correctness_by_validator_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_validator_balance_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_validator_balance_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS `${NETWORK_NAME}`.fct_sync_committee_participation_by_validator_daily_local ON CLUSTER '{cluster}';
