-- fct_attestation_vote_correctness_by_validator now backfills to genesis, before attestation gossip exists. Its
-- source_correct comes from gossip only, so it is NULL before gossip, and the source_correct_count built on it counts
-- only votes seen on gossip.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE fct_attestation_vote_correctness_by_validator_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct` 'Whether the source vote was correct, from the attestation seen on gossip. NULL if gossip did not see it, which is every slot before gossip collection began';

ALTER TABLE fct_attestation_vote_correctness_by_validator ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct` 'Whether the source vote was correct, from the attestation seen on gossip. NULL if gossip did not see it, which is every slot before gossip collection began';

ALTER TABLE fct_attestation_vote_correctness_by_validator_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_validator_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_validator_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_validator_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_validator_weekly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_validator_weekly ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Number of source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_client_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_client_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_client_pair_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Source votes that were correct, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_client_pair_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `source_correct_count` 'Source votes that were correct, from attestations seen on gossip';
