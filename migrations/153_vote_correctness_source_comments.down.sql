ALTER TABLE fct_attestation_vote_correctness_by_validator_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct` 'Whether the source vote was correct. NULL if not attested';

ALTER TABLE fct_attestation_vote_correctness_by_validator ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct` 'Whether the source vote was correct. NULL if not attested';

ALTER TABLE fct_attestation_vote_correctness_by_validator_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_validator_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_validator_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_validator_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_validator_weekly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_validator_weekly ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_client_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_client_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_client_pair_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Source votes that were correct';

ALTER TABLE fct_attestation_vote_correctness_by_client_pair_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Source votes that were correct';
