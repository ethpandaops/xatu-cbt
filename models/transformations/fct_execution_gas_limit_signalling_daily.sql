---
table: fct_execution_gas_limit_signalling_daily
type: incremental
interval:
  type: slot
  max: 604800
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1h"
tags:
  - daily
  - execution
  - gas
dependencies:
  - "{{external}}.mev_relay_validator_registration"
---
-- Daily snapshots of validator gas limit signalling using a rolling 7-day window.
-- Takes each validator's max gas_limit from the last 7 days at each day.
-- Output is a single row per day with a Map of gas_limit_band -> validator_count.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Pre-aggregate to daily max per validator (single table scan, no FINAL needed)
    validator_daily_max AS (
        SELECT
            toDate(event_date_time) AS event_day,
            validator_index,
            max(gas_limit) AS max_gas_limit
        FROM {{ index .dep "{{external}}" "mev_relay_validator_registration" "helpers" "from" }}
        WHERE event_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 7 DAY
          AND event_date_time < fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 DAY
        GROUP BY event_day, validator_index
    ),
    -- Expand each daily record to all target days it contributes to (rolling 7-day window)
    rolling_validator_gas AS (
        SELECT
            v.event_day + INTERVAL (7 - window_offset) DAY AS target_day,
            v.validator_index,
            v.max_gas_limit
        FROM validator_daily_max v
        ARRAY JOIN range(1, 8) AS window_offset
        WHERE v.event_day + INTERVAL (7 - window_offset) DAY >= toDate(fromUnixTimestamp({{ .bounds.start }}))
          AND v.event_day + INTERVAL (7 - window_offset) DAY <= toDate(fromUnixTimestamp({{ .bounds.end }}))
    ),
    -- Get max per validator per target day
    validator_target_max AS (
        SELECT
            target_day,
            validator_index,
            max(max_gas_limit) AS max_gas_limit
        FROM rolling_validator_gas
        GROUP BY target_day, validator_index
    ),
    -- Group into gas limit bands (1M increments)
    daily_bands AS (
        SELECT
            target_day,
            toString(toUInt64(ceil(max_gas_limit / 1000000) * 1000000)) AS gas_limit_band,
            toUInt32(count()) AS validator_count
        FROM validator_target_max
        GROUP BY target_day, gas_limit_band
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    target_day AS day_start_date,
    mapFromArrays(
        groupArray(gas_limit_band),
        groupArray(validator_count)
    ) AS gas_limit_band_counts
FROM daily_bands
GROUP BY target_day
