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
  forwardfill: "@every 5s"
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
    day_bounds AS (
        SELECT
            toDate(min(event_date_time)) AS min_day,
            toDate(max(event_date_time)) AS max_day
        FROM {{ index .dep "{{external}}" "mev_relay_validator_registration" "helpers" "from" }} FINAL
        WHERE event_date_time >= fromUnixTimestamp({{ .bounds.start }})
          AND event_date_time < fromUnixTimestamp({{ .bounds.end }})
    ),
    days AS (
        SELECT arrayJoin(
            arrayMap(x -> (SELECT min_day FROM day_bounds) + INTERVAL x DAY,
                     range(toUInt32(dateDiff('day',
                         (SELECT min_day FROM day_bounds),
                         (SELECT max_day FROM day_bounds)))))
        ) AS day
    ),
    -- For each day, get each validator's max gas_limit from last 7 days
    rolling_7d_validator_gas AS (
        SELECT
            d.day,
            r.validator_index,
            max(r.gas_limit) AS max_gas_limit
        FROM days d
        INNER JOIN {{ index .dep "{{external}}" "mev_relay_validator_registration" "helpers" "from" }} r FINAL
            ON toDate(r.event_date_time) >= d.day - INTERVAL 6 DAY
           AND toDate(r.event_date_time) <= d.day
        WHERE r.event_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 7 DAY
          AND r.event_date_time < fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 DAY
        GROUP BY d.day, r.validator_index
    ),
    daily_bands AS (
        SELECT
            day,
            toString(toUInt64(ceil(max_gas_limit / 1000000) * 1000000)) AS gas_limit_band,
            toUInt32(count()) AS validator_count
        FROM rolling_7d_validator_gas
        GROUP BY day, gas_limit_band
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day AS day_start_date,
    mapFromArrays(
        groupArray(gas_limit_band),
        groupArray(validator_count)
    ) AS gas_limit_band_counts
FROM daily_bands
GROUP BY day
