DROP TABLE IF EXISTS fct_validator_count_by_credential_type_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_count_by_credential_type_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_validator_effective_balance_by_credential_type_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_effective_balance_by_credential_type_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_validator_stake_by_entity_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_stake_by_entity_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_validator_queue_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_queue_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_validator_operation_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_operation_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_attestation_reward_by_validator_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_attestation_reward_by_validator_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_sync_committee_reward_by_validator_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_sync_committee_reward_by_validator_daily_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_block_proposer_reward ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_block_proposer_reward_local ON CLUSTER '{cluster}';

DROP TABLE IF EXISTS fct_validator_reward_total_daily ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS fct_validator_reward_total_daily_local ON CLUSTER '{cluster}';
