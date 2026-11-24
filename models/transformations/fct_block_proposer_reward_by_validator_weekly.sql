---
table: fct_block_proposer_reward_by_validator_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - block
  - reward
  - validator_performance
dependencies:
  - "{{transformation}}.fct_block_proposer_reward"
---
-- Consensus layer proposer rewards per proposer per week (Monday to Sunday, UTC),
-- summed from fct_block_proposer_reward. Each run rewrites every week it touches
-- in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    period_bounds AS (
        SELECT
            toMonday(toDate(fromUnixTimestamp({{ .bounds.start }}))) AS min_period,
            toMonday(toDate(fromUnixTimestamp({{ .bounds.end }}))) AS max_period
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toMonday(toDate(slot_start_date_time)) AS week_start_date,
    proposer_index,
    toUInt32(count()) AS block_count,
    sum(total) AS total,
    sum(attestations) AS attestations,
    sum(sync_aggregate) AS sync_aggregate,
    sum(proposer_slashings) AS proposer_slashings,
    sum(attester_slashings) AS attester_slashings
FROM {{ index .dep "{{transformation}}" "fct_block_proposer_reward" "helpers" "from" }} FINAL
WHERE slot_start_date_time >= toDateTime((SELECT min_period FROM period_bounds))
  AND slot_start_date_time < toDateTime((SELECT max_period FROM period_bounds)) + INTERVAL 7 DAY
GROUP BY week_start_date, proposer_index
