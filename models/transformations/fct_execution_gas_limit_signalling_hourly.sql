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
  forwardfill: "@every 5s"
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
    hour_bounds AS (
        SELECT
            toStartOfHour(min(event_date_time)) AS min_hour,
            toStartOfHour(max(event_date_time)) AS max_hour
        FROM {{ index .dep "{{external}}" "mev_relay_validator_registration" "helpers" "from" }} FINAL
        WHERE event_date_time >= fromUnixTimestamp({{ .bounds.start }})
          AND event_date_time < fromUnixTimestamp({{ .bounds.end }})
    ),
    hours AS (
        SELECT arrayJoin(
            arrayMap(x -> (SELECT min_hour FROM hour_bounds) + INTERVAL x HOUR,
                     range(toUInt32(dateDiff('hour',
                         (SELECT min_hour FROM hour_bounds),
                         (SELECT max_hour FROM hour_bounds)))))
        ) AS hour
    ),
    -- For each hour, get each validator's max gas_limit from last 7 days
    rolling_7d_validator_gas AS (
        SELECT
            h.hour,
            r.validator_index,
            max(r.gas_limit) AS max_gas_limit
        FROM hours h
        INNER JOIN {{ index .dep "{{external}}" "mev_relay_validator_registration" "helpers" "from" }} r FINAL
            ON r.event_date_time >= h.hour - INTERVAL 7 DAY
           AND r.event_date_time < h.hour + INTERVAL 1 HOUR
        WHERE r.event_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 7 DAY
          AND r.event_date_time < fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 HOUR
        GROUP BY h.hour, r.validator_index
    ),
    hourly_bands AS (
        SELECT
            hour,
            toString(toUInt64(ceil(max_gas_limit / 1000000) * 1000000)) AS gas_limit_band,
            toUInt32(count()) AS validator_count
        FROM rolling_7d_validator_gas
        GROUP BY hour, gas_limit_band
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour AS hour_start_date_time,
    mapFromArrays(
        groupArray(gas_limit_band),
        groupArray(validator_count)
    ) AS gas_limit_band_counts
FROM hourly_bands
GROUP BY hour
