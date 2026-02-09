---
table: fct_validator_balance
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
  - balance
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_validators"
---
-- Per-epoch validator balance and status. One row per validator per epoch.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    epoch,
    epoch_start_date_time,
    `index` AS validator_index,
    balance,
    effective_balance,
    status,
    slashed
FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND meta_network_name = '{{ .env.NETWORK }}'
