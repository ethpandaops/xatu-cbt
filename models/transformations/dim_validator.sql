---
table: dim_validator
type: incremental
interval:
  type: slot
  max: 3600
schedules:
  forwardfill: "@every 1h"
tags:
  - validator
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_validators"
  - "{{external}}.canonical_beacon_validators_withdrawal_credentials"
  - "{{external}}.canonical_beacon_validators_pubkeys"
---
-- Current state of every validator, taken from the latest epoch in the interval.
-- This is a current-state table, so it only forward fills and the newest epoch
-- wins (the ReplacingMergeTree version is the epoch).
--
-- Unset lifecycle epochs (FAR_FUTURE_EPOCH) are written as NULL. Older Xatu data
-- stores both epoch 0 and FAR_FUTURE_EPOCH as NULL, newer data stores the real
-- values. NULL is told apart by status: a validator that has activated has a real
-- activation epoch, so NULL there means 0 (genesis validators). Exit and
-- withdrawable epochs are never 0, so NULL there means not set.
--
-- canonical_beacon_validators_withdrawal_credentials keeps one row per distinct
-- (index, credentials) and refreshes the epoch of the credentials still in use,
-- so the current credentials are the ones seen at the latest epoch. Credential
-- changes only go 0x00 -> 0x01 -> 0x02, which breaks any tie.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- The latest epoch in the interval, if its snapshot is complete: it holds every
    -- validator index from 0 to its highest index, and that highest index is not
    -- below the previous epoch's (validators are never removed). Otherwise nothing
    -- is written and the next run tries a later epoch.
    latest AS (
        SELECT max(epoch_start_date_time) AS snapshot_time
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
        WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND `index` = 0
    ),
    sizes AS (
        SELECT epoch_start_date_time, uniqExact(`index`) AS validators, max(`index`) AS max_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time IN (
            (SELECT snapshot_time FROM latest),
            (SELECT snapshot_time FROM latest) - INTERVAL 384 SECOND
          )
        GROUP BY epoch_start_date_time
    ),
    snapshot AS (
        SELECT c.epoch_start_date_time AS snapshot_time
        FROM sizes AS c
        GLOBAL LEFT JOIN sizes AS p ON p.epoch_start_date_time = c.epoch_start_date_time - INTERVAL 384 SECOND
        WHERE c.epoch_start_date_time = (SELECT snapshot_time FROM latest)
          AND c.validators = c.max_index + 1
          AND c.max_index >= p.max_index
    ),
    validators AS (
        SELECT
            `index` AS validator_index,
            epoch,
            epoch_start_date_time,
            status,
            balance,
            effective_balance,
            slashed,
            activation_eligibility_epoch,
            activation_epoch,
            exit_epoch,
            withdrawable_epoch
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE epoch_start_date_time = (SELECT snapshot_time FROM snapshot)
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),
    credentials AS (
        SELECT
            `index` AS validator_index,
            argMax(withdrawal_credentials, (epoch, withdrawal_credentials)) AS withdrawal_credentials
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators_withdrawal_credentials" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY `index`
    ),
    pubkeys AS (
        SELECT
            `index` AS validator_index,
            argMax(pubkey, epoch) AS pubkey
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators_pubkeys" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY `index`
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    v.epoch AS epoch,
    v.epoch_start_date_time AS epoch_start_date_time,
    v.validator_index AS validator_index,
    p.pubkey AS pubkey,
    c.withdrawal_credentials AS withdrawal_credentials,
    substring(c.withdrawal_credentials, 1, 4) AS credential_type,
    if(credential_type IN ('0x01', '0x02'), concat('0x', substring(c.withdrawal_credentials, 27, 40)), NULL) AS withdrawal_address,
    v.status AS status,
    v.balance AS balance,
    v.effective_balance AS effective_balance,
    v.slashed AS slashed,
    multiIf(v.activation_eligibility_epoch = 18446744073709551615, NULL, v.activation_eligibility_epoch IS NULL AND v.status != 'pending_initialized', toNullable(toUInt64(0)), v.activation_eligibility_epoch) AS activation_eligibility_epoch,
    multiIf(v.activation_epoch = 18446744073709551615, NULL, v.activation_epoch IS NULL AND v.status NOT IN ('pending_initialized', 'pending_queued'), toNullable(toUInt64(0)), v.activation_epoch) AS activation_epoch,
    if(v.exit_epoch = 18446744073709551615, NULL, v.exit_epoch) AS exit_epoch,
    if(v.withdrawable_epoch = 18446744073709551615, NULL, v.withdrawable_epoch) AS withdrawable_epoch
FROM validators AS v
GLOBAL LEFT JOIN credentials AS c ON v.validator_index = c.validator_index
GLOBAL LEFT JOIN pubkeys AS p ON v.validator_index = p.validator_index
SETTINGS join_use_nulls = 0
