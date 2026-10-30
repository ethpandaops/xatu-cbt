---
table: fct_attestation_reward_by_validator_daily
type: incremental
interval:
  type: slot
  min: 21600
  max: 86400
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - attestation
  - reward
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_attestation_reward"
---
-- Attestation rewards and penalties per validator per day, from the Beacon API
-- attestation rewards Cannon stores for every active validator in every epoch
-- (from Altair). Each run rewrites every day it touches in full. Runs wait for
-- at least 6 hours of new data, since a day holds ~180M source rows on mainnet.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(epoch_start_date_time) AS day_start_date,
    validator_index,
    toUInt32(count()) AS epoch_count,
    sum(h) AS head,
    sum(t) AS target,
    sum(s) AS source,
    sum(i) AS inactivity,
    sum(h + t + s + i) AS total,
    sum(least(h, 0) + least(t, 0) + least(s, 0) + least(i, 0)) AS penalty,
    toUInt32(countIf(h < 0 OR t < 0 OR s < 0 OR i < 0)) AS penalized_epoch_count
FROM (
    SELECT epoch_start_date_time, validator_index, head AS h, target AS t, source AS s, inactivity AS i
    FROM {{ index .dep "{{external}}" "canonical_beacon_attestation_reward" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
      AND epoch_start_date_time >= toDateTime((SELECT min_day FROM day_bounds))
      AND epoch_start_date_time < toDateTime((SELECT max_day FROM day_bounds)) + INTERVAL 1 DAY
)
GROUP BY day_start_date, validator_index
SETTINGS max_bytes_before_external_group_by = 10000000000
