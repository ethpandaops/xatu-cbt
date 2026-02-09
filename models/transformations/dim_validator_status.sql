---
table: dim_validator_status
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - epoch
  - validator
  - status
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_validators"
  - "{{transformation}}.dim_validator_pubkey"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    status_transitions AS (
        SELECT
            `index` AS validator_index,
            status,
            min(epoch) AS first_epoch,
            min(epoch_start_date_time) AS first_epoch_start_date_time,
            argMin(activation_epoch, epoch) AS activation_epoch,
            argMin(activation_eligibility_epoch, epoch) AS activation_eligibility_epoch,
            argMin(exit_epoch, epoch) AS exit_epoch,
            argMin(withdrawable_epoch, epoch) AS withdrawable_epoch,
            argMin(slashed, epoch) AS slashed
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY `index`, status
    ),
    pubkeys AS (
        SELECT
            validator_index,
            pubkey
        FROM {{ index .dep "{{transformation}}" "dim_validator_pubkey" "helpers" "from" }} FINAL
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt32(4294967295 - st.first_epoch) AS version,
    st.validator_index,
    coalesce(p.pubkey, '') AS pubkey,
    st.status,
    st.first_epoch AS epoch,
    st.first_epoch_start_date_time AS epoch_start_date_time,
    st.activation_epoch,
    st.activation_eligibility_epoch,
    st.exit_epoch,
    st.withdrawable_epoch,
    st.slashed
FROM status_transitions st
LEFT JOIN pubkeys p ON st.validator_index = p.validator_index
