-- total is head + target + source + inclusion_delay + inactivity, so compute it on
-- read instead of storing it: it was the largest column of this ~117B row table.
ALTER TABLE fct_attestation_reward_by_validator ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `total`;

ALTER TABLE fct_attestation_reward_by_validator_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `total`;

ALTER TABLE fct_attestation_reward_by_validator_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `total` Int64 ALIAS head + target + source + inclusion_delay + inactivity COMMENT 'head + target + source + inclusion_delay + inactivity in Gwei' AFTER `inactivity`;

ALTER TABLE fct_attestation_reward_by_validator ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `total` Int64 ALIAS head + target + source + inclusion_delay + inactivity COMMENT 'head + target + source + inclusion_delay + inactivity in Gwei' AFTER `inactivity`;
