---
table: fct_validator_balance_daily
type: incremental
interval:
  type: day
  max: 30
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 10m"
tags:
  - daily
  - validator
  - balance
dependencies:
  - "{{external}}.canonical_beacon_validators"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Aggregate validator data per day (~225 epochs per day on mainnet)
WITH daily_validators AS (
    SELECT
        toDate(epoch_start_date_time) AS date,
        `index` AS validator_index,
        epoch,
        epoch_start_date_time,
        balance,
        effective_balance,
        status,
        slashed
    FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
    WHERE epoch_start_date_time >= toStartOfDay(fromUnixTimestamp({{ .bounds.start }}))
        AND epoch_start_date_time < toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY
        AND meta_network_name = '{{ .env.NETWORK }}'
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    date,
    validator_index,
    -- Epoch range for the day
    min(epoch) AS start_epoch,
    max(epoch) AS end_epoch,
    -- Balance at start of day (first epoch)
    argMin(balance, epoch) AS start_balance,
    -- Balance at end of day (last epoch)
    argMax(balance, epoch) AS end_balance,
    -- Min/max balance during the day
    min(balance) AS min_balance,
    max(balance) AS max_balance,
    -- End of day state
    argMax(effective_balance, epoch) AS effective_balance,
    argMax(status, epoch) AS status,
    argMax(slashed, epoch) AS slashed
FROM daily_validators
GROUP BY date, validator_index

