---
table: fct_validator_balance_hourly
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 10m"
tags:
  - hourly
  - validator
  - balance
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_validators"
---
-- This query expands the epoch range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. The ReplacingMergeTree
-- will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries for the current epoch range using epoch timestamps
    hour_bounds AS (
        SELECT
            toStartOfHour(min(epoch_start_date_time)) AS min_hour,
            toStartOfHour(max(epoch_start_date_time)) AS max_hour
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),

    -- Validator data for all epochs within the hour boundaries
    hourly_validators AS (
        SELECT
            toStartOfHour(epoch_start_date_time) AS hour_start_date_time,
            `index` AS validator_index,
            epoch,
            epoch_start_date_time,
            balance,
            effective_balance,
            status,
            slashed
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE epoch_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND epoch_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND meta_network_name = '{{ .env.NETWORK }}'
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    validator_index,
    -- Epoch range for the hour
    min(epoch) AS start_epoch,
    max(epoch) AS end_epoch,
    -- Balance at start of hour (first epoch)
    argMin(balance, epoch) AS start_balance,
    -- Balance at end of hour (last epoch)
    argMax(balance, epoch) AS end_balance,
    -- Min/max balance during the hour
    min(balance) AS min_balance,
    max(balance) AS max_balance,
    -- End of hour state
    argMax(effective_balance, epoch) AS effective_balance,
    argMax(status, epoch) AS status,
    argMax(slashed, epoch) AS slashed
FROM hourly_validators
GROUP BY hour_start_date_time, validator_index
