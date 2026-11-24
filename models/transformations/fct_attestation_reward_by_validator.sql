---
table: fct_attestation_reward_by_validator
type: incremental
interval:
  type: slot
  max: 21600
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - epoch
  - attestation
  - reward
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_attestation_reward"
  - "{{transformation}}.fct_attestation_vote_correctness_by_validator"
---
-- Attestation reward of each validator in each epoch, from the attestation
-- rewards Cannon stores (Beacon API from Altair, computed from beacon states for
-- phase0, which has an inclusion delay component), keyed by validator so one validator's epochs
-- are a primary key range. It covers the same range as the per-duty rows in
-- fct_attestation_vote_correctness_by_validator (a dependency for its bounds).
-- A reward never changes once Cannon stores it, so the table has no
-- updated_date_time, and total is an ALIAS column computed on read: at about 117B
-- rows on mainnet each stored column costs tens of GiB.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    epoch,
    epoch_start_date_time,
    validator_index,
    head,
    target,
    source,
    inclusion_delay,
    inactivity
FROM (
    SELECT epoch, epoch_start_date_time, validator_index, head, target, source, toInt64(0) AS inclusion_delay, inactivity
    FROM {{ index .dep "{{external}}" "canonical_beacon_attestation_reward" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
      AND epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    UNION ALL
    SELECT epoch, epoch_start_date_time, validator_index, head, target, source, toInt64(inclusion_delay) AS inclusion_delay, inactivity
    FROM {{ index .dep "{{external}}" "canonical_beacon_attestation_reward" "helpers" "from" | replace "`canonical_beacon_attestation_reward" "`canonical_beacon_phase0_attestation_reward" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
      AND epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
