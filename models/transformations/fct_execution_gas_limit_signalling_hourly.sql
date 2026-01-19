---
table: fct_execution_gas_limit_signalling_hourly
type: incremental
interval:
  type: slot
  max: 25200
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5m"
tags:
  - hourly
  - execution
  - gas
dependencies:
  - "{{external}}.mev_relay_validator_registration"
---
-- Hourly snapshots of validator gas limit signalling using a rolling 7-day window.
-- Takes each validator's max gas_limit from the last 7 days at each hour.
-- Output is a single row per hour with a Map of gas_limit_band -> validator_count.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Generate target hours from bounds
    target_hours AS (
        SELECT toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) + INTERVAL number HOUR AS target_hour
        FROM numbers(toUInt32(dateDiff('hour',
            toStartOfHour(fromUnixTimestamp({{ .bounds.start }})),
            toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
        )) + 1)
    ),
    -- Pre-aggregate to daily max per validator (single table scan, no FINAL needed)
    -- Daily aggregation is sufficient since gas_limit is stable per validator
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
    -- For each target hour, get validators with data in the 7-day window ending at that hour
    -- A daily record contributes to an hour if: event_day is within [target_hour - 7 days, target_hour]
    rolling_validator_gas AS (
        SELECT
            h.target_hour,
            v.validator_index,
            max(v.max_gas_limit) AS max_gas_limit
        FROM target_hours h
        INNER JOIN validator_daily_max v
            ON v.event_day >= toDate(h.target_hour - INTERVAL 7 DAY)
           AND v.event_day <= toDate(h.target_hour)
        GROUP BY h.target_hour, v.validator_index
    ),
    -- Group into gas limit bands (1M increments)
    hourly_bands AS (
        SELECT
            target_hour,
            toString(toUInt64(ceil(max_gas_limit / 1000000) * 1000000)) AS gas_limit_band,
            toUInt32(count()) AS validator_count
        FROM rolling_validator_gas
        GROUP BY target_hour, gas_limit_band
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    target_hour AS hour_start_date_time,
    mapFromArrays(
        groupArray(gas_limit_band),
        groupArray(validator_count)
    ) AS gas_limit_band_counts
FROM hourly_bands
GROUP BY target_hour
