DROP TABLE IF EXISTS fct_validator_count_by_credential_type_weekly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_count_by_credential_type_weekly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_validator_queue_weekly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_queue_weekly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_validator_operation_weekly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_operation_weekly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_validator_reward_total_weekly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_reward_total_weekly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_attestation_reward_by_validator_weekly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_attestation_reward_by_validator_weekly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_sync_committee_reward_by_validator_weekly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_sync_committee_reward_by_validator_weekly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_block_proposer_reward_by_validator_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_block_proposer_reward_by_validator_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_block_proposer_reward_by_validator_weekly ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_block_proposer_reward_by_validator_weekly_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_attestation_reward_by_validator ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_attestation_reward_by_validator_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_sync_committee_reward_by_validator ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_sync_committee_reward_by_validator_local ON CLUSTER '{cluster}';
