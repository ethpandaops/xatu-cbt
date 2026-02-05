---
table: fct_validator_balance_daily
type: incremental
interval:
  type: slot
  max: 100000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 10m"
tags:
  - daily
  - validator
  - balance
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_validators"
---
-- This query expands the epoch range to complete day boundaries to handle partial
-- day aggregations at the head of incremental processing. The ReplacingMergeTree
-- will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the day boundaries for the current epoch range using epoch timestamps
    day_bounds AS (
        SELECT
            toDate(min(epoch_start_date_time)) AS min_day,
            toDate(max(epoch_start_date_time)) AS max_day
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),

    -- Validator data for all epochs within the day boundaries
    daily_validators AS (
        SELECT
            toDate(epoch_start_date_time) AS day_start_date,
            `index` AS validator_index,
            epoch,
            epoch_start_date_time,
            balance,
            effective_balance,
            status,
            slashed
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE epoch_start_date_time >= toDateTime((SELECT min_day FROM day_bounds))
          AND epoch_start_date_time < toDateTime((SELECT max_day FROM day_bounds)) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
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
GROUP BY day_start_date, validator_index
