---
table: fct_proposer_reward_hourly
type: incremental
interval:
  type: slot
  max: 25200
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - consensus
  - mev
dependencies:
  - "{{transformation}}.fct_block_mev"
---
-- Hourly aggregation of proposer reward (MEV relay block value).
-- Converts wei values to ETH and computes percentiles, Bollinger bands, and
-- 6-hour moving averages. Only covers MEV relay blocks, not locally built blocks.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_block_mev" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND status = 'canonical'
          AND value IS NOT NULL
          AND value > 0
    ),
    slots_in_hours AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            toFloat64(value) / 1e18 AS reward_eth
        FROM {{ index .dep "{{transformation}}" "fct_block_mev" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND status = 'canonical'
          AND value IS NOT NULL
          AND value > 0
    ),
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            reward_eth,
            avg(reward_eth) OVER (ORDER BY slot_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_reward_eth
        FROM slots_in_hours
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    count() AS block_count,
    round(sum(reward_eth), 6) AS total_reward_eth,
    round(avg(reward_eth), 6) AS avg_reward_eth,
    round(min(reward_eth), 6) AS min_reward_eth,
    round(max(reward_eth), 6) AS max_reward_eth,
    round(quantile(0.05)(reward_eth), 6) AS p05_reward_eth,
    round(quantile(0.50)(reward_eth), 6) AS p50_reward_eth,
    round(quantile(0.95)(reward_eth), 6) AS p95_reward_eth,
    round(stddevPop(reward_eth), 6) AS stddev_reward_eth,
    round(avg(reward_eth) + 2 * stddevPop(reward_eth), 6) AS upper_band_reward_eth,
    round(avg(reward_eth) - 2 * stddevPop(reward_eth), 6) AS lower_band_reward_eth,
    round(avg(ma_reward_eth), 6) AS moving_avg_reward_eth
FROM slots_with_ma
GROUP BY toStartOfHour(slot_start_date_time)
