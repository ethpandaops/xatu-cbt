---
table: fct_validator_balance_hourly
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - hourly
  - validator
  - balance
  - validator_performance
dependencies:
  - "{{transformation}}.fct_validator_balance"
---
-- This query expands the epoch range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. The ReplacingMergeTree
-- will merge duplicates keeping the latest row.
--
-- Sources from the per-epoch table rather than raw external data.
-- Uses argMax dedup instead of FINAL so the p_by_epoch_start_date_time projection is used.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries for the current epoch range
    -- No dedup needed: duplicates have identical epoch_start_date_time so min/max is unaffected
    hour_bounds AS (
        SELECT
            toStartOfHour(min(epoch_start_date_time)) AS min_hour,
            toStartOfHour(max(epoch_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_validator_balance" "helpers" "from" }}
        WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),

    -- Deduplicated per-epoch data within the expanded hour boundaries
    -- Uses argMax instead of FINAL so the projection on epoch_start_date_time is used
    per_epoch AS (
        SELECT
            validator_index,
            epoch,
            epoch_start_date_time,
            argMax(balance, updated_date_time) AS balance,
            argMax(effective_balance, updated_date_time) AS effective_balance,
            argMax(status, updated_date_time) AS status,
            argMax(slashed, updated_date_time) AS slashed
        FROM {{ index .dep "{{transformation}}" "fct_validator_balance" "helpers" "from" }}
        WHERE epoch_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND epoch_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
        GROUP BY validator_index, epoch, epoch_start_date_time
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(epoch_start_date_time) AS hour_start_date_time,
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
FROM per_epoch
GROUP BY hour_start_date_time, validator_index
