---
table: fct_sync_committee_reward_by_validator_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - sync_committee
  - reward
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_sync_committee_reward"
---
-- Sync committee rewards and penalties per validator per day, from the Beacon API
-- sync committee rewards Cannon stores for every block (from Altair). A missed
-- sync committee vote is a negative reward. Each run rewrites every day it
-- touches in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
    validator_index,
    toUInt32(count()) AS slot_count,
    sum(r) AS total,
    sumIf(r, r > 0) AS reward,
    sumIf(r, r < 0) AS penalty,
    toUInt32(countIf(r < 0)) AS penalized_slot_count
FROM (
    SELECT slot_start_date_time, validator_index, reward AS r
    FROM {{ index .dep "{{external}}" "canonical_beacon_sync_committee_reward" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
      AND slot_start_date_time >= toDateTime((SELECT min_day FROM day_bounds))
      AND slot_start_date_time < toDateTime((SELECT max_day FROM day_bounds)) + INTERVAL 1 DAY
)
GROUP BY day_start_date, validator_index
