-- Columns live on both the _local and the Distributed table, so both are updated.

ALTER TABLE fct_attestation_vote_correctness_by_validator_hourly_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `source_incorrect_count` UInt32 COMMENT 'Number of source votes that were incorrect, from attestations seen on gossip' CODEC(ZSTD(1));

ALTER TABLE fct_attestation_vote_correctness_by_validator_hourly ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `source_incorrect_count` UInt32 COMMENT 'Number of source votes that were incorrect, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_validator_daily_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `source_incorrect_count` UInt32 COMMENT 'Number of source votes that were incorrect, from attestations seen on gossip' CODEC(ZSTD(1));

ALTER TABLE fct_attestation_vote_correctness_by_validator_daily ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `source_incorrect_count` UInt32 COMMENT 'Number of source votes that were incorrect, from attestations seen on gossip';

ALTER TABLE fct_attestation_vote_correctness_by_validator_weekly_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `source_incorrect_count` UInt32 COMMENT 'Number of source votes that were incorrect, from attestations seen on gossip' CODEC(ZSTD(1));

ALTER TABLE fct_attestation_vote_correctness_by_validator_weekly ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `source_incorrect_count` UInt32 COMMENT 'Number of source votes that were incorrect, from attestations seen on gossip';
