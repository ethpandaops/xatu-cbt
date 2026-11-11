---
table: fct_validator_queue_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - validator
  - queue
dependencies:
  - "{{external}}.canonical_beacon_validators"
  - "{{external}}.canonical_beacon_state_pending_deposit"
  - "{{external}}.canonical_beacon_state_pending_consolidation"
  - "{{external}}.canonical_beacon_state_pending_partial_withdrawal"
---
-- Activation and exit queues and the Electra pending queues, from one snapshot per
-- day. The pending queues only exist from Electra, and an empty queue has no rows,
-- so they are 0 before Electra and when empty.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- One snapshot per day: the last epoch of the day, or the latest epoch so far
    -- for today. A day is only written once that epoch's snapshot is complete:
    -- it holds every validator index from 0 to its highest index, and that highest
    -- index is not below the previous epoch's (validators are never removed). Days
    -- before the interval that are missing from this table (skipped because their
    -- snapshot was incomplete) are retried, up to 7 per run. The version is the
    -- snapshot epoch, so a run that only reaches an earlier epoch of a day cannot
    -- replace a later snapshot.
    retry_days AS (
        SELECT day_start_date
        FROM (
            SELECT ifNull((SELECT min(day_start_date) FROM `{{ .self.database }}`.`{{ .self.table }}`), toDate(0)) + number AS day_start_date
            FROM numbers(toUInt64(greatest(0, dateDiff('day',
                ifNull((SELECT min(day_start_date) FROM `{{ .self.database }}`.`{{ .self.table }}`), toDate(0)),
                toDate(fromUnixTimestamp({{ .bounds.start }}))))))
        )
        WHERE (SELECT count() FROM `{{ .self.database }}`.`{{ .self.table }}`) > 0
          AND day_start_date GLOBAL NOT IN (SELECT DISTINCT day_start_date FROM `{{ .self.database }}`.`{{ .self.table }}`)
        ORDER BY day_start_date
        LIMIT 7
    ),
    day_ends AS (
        SELECT
            toDate(epoch_start_date_time) AS day_start_date,
            max(epoch) AS snapshot_epoch,
            max(epoch_start_date_time) AS snapshot_time
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND `index` = 0
          AND (
            epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            OR (
                epoch_start_date_time >= toDateTime((SELECT min(day_start_date) FROM retry_days))
                AND epoch_start_date_time < toDateTime((SELECT max(day_start_date) FROM retry_days)) + INTERVAL 1 DAY
                AND toDate(epoch_start_date_time) GLOBAL IN (SELECT day_start_date FROM retry_days)
            )
          )
        GROUP BY day_start_date
    ),
    snapshot_sizes AS (
        SELECT epoch_start_date_time, uniqExact(`index`) AS validators, max(`index`) AS max_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (
            SELECT snapshot_time FROM day_ends
            UNION ALL
            SELECT snapshot_time - INTERVAL 384 SECOND FROM day_ends
          )
        GROUP BY epoch_start_date_time
    ),
    snapshots AS (
        SELECT d.day_start_date AS day_start_date, d.snapshot_epoch AS snapshot_epoch, d.snapshot_time AS snapshot_time
        FROM day_ends AS d
        GLOBAL INNER JOIN snapshot_sizes AS c ON c.epoch_start_date_time = d.snapshot_time
        GLOBAL LEFT JOIN snapshot_sizes AS p ON p.epoch_start_date_time = d.snapshot_time - INTERVAL 384 SECOND
        WHERE c.validators = c.max_index + 1
          AND (p.max_index IS NULL OR c.max_index >= p.max_index)
    ),
    validator_queues AS (
        SELECT
            epoch_start_date_time,
            countIf(status = 'pending_initialized') AS pending_initialized_count,
            sumIf(ifNull(balance, 0), status = 'pending_initialized') AS pending_initialized_balance,
            countIf(status = 'pending_queued') AS activation_queue_count,
            sumIf(ifNull(effective_balance, 0), status = 'pending_queued') AS activation_queue_effective_balance,
            countIf(status IN ('active_exiting', 'active_slashed')) AS exit_queue_count,
            sumIf(ifNull(effective_balance, 0), status IN ('active_exiting', 'active_slashed')) AS exit_queue_effective_balance
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (SELECT snapshot_time FROM snapshots)
        GROUP BY epoch_start_date_time
    ),
    deposits AS (
        SELECT epoch_start_date_time, count() AS pending_deposit_count, sum(toUInt64(amount)) AS pending_deposit_amount
        FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_deposit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (SELECT snapshot_time FROM snapshots)
        GROUP BY epoch_start_date_time
    ),
    consolidation_queue AS (
        SELECT epoch_start_date_time, source_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_consolidation" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (SELECT snapshot_time FROM snapshots)
    ),
    consolidations AS (
        -- The balance a consolidation moves is the source's effective balance (or
        -- balance, if lower) when it is processed. This is the sources' effective
        -- balance at the snapshot.
        SELECT
            q.epoch_start_date_time AS epoch_start_date_time,
            count() AS pending_consolidation_count,
            sum(ifNull(v.effective_balance, 0)) AS pending_consolidation_source_effective_balance
        FROM consolidation_queue AS q
        GLOBAL LEFT JOIN (
            SELECT epoch_start_date_time, `index`, effective_balance
            FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
            WHERE meta_network_name = '{{ .env.NETWORK }}'
              AND epoch_start_date_time GLOBAL IN (SELECT snapshot_time FROM snapshots)
              AND `index` GLOBAL IN (SELECT source_index FROM consolidation_queue)
        ) AS v ON v.epoch_start_date_time = q.epoch_start_date_time AND v.`index` = q.source_index
        GROUP BY q.epoch_start_date_time
    ),
    partial_withdrawals AS (
        SELECT epoch_start_date_time, count() AS pending_partial_withdrawal_count, sum(toUInt64(amount)) AS pending_partial_withdrawal_amount
        FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_partial_withdrawal" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (SELECT snapshot_time FROM snapshots)
        GROUP BY epoch_start_date_time
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    s.day_start_date AS day_start_date,
    s.snapshot_epoch AS epoch,
    s.snapshot_time AS epoch_start_date_time,
    toUInt32(v.pending_initialized_count) AS pending_initialized_count,
    toUInt64(v.pending_initialized_balance) AS pending_initialized_balance,
    toUInt32(v.activation_queue_count) AS activation_queue_count,
    toUInt64(v.activation_queue_effective_balance) AS activation_queue_effective_balance,
    toUInt32(v.exit_queue_count) AS exit_queue_count,
    toUInt64(v.exit_queue_effective_balance) AS exit_queue_effective_balance,
    toUInt32(ifNull(d.pending_deposit_count, 0)) AS pending_deposit_count,
    toUInt64(ifNull(d.pending_deposit_amount, 0)) AS pending_deposit_amount,
    toUInt32(ifNull(c.pending_consolidation_count, 0)) AS pending_consolidation_count,
    toUInt64(ifNull(c.pending_consolidation_source_effective_balance, 0)) AS pending_consolidation_source_effective_balance,
    toUInt32(ifNull(p.pending_partial_withdrawal_count, 0)) AS pending_partial_withdrawal_count,
    toUInt64(ifNull(p.pending_partial_withdrawal_amount, 0)) AS pending_partial_withdrawal_amount
FROM snapshots AS s
GLOBAL INNER JOIN validator_queues AS v ON v.epoch_start_date_time = s.snapshot_time
GLOBAL LEFT JOIN deposits AS d ON d.epoch_start_date_time = s.snapshot_time
GLOBAL LEFT JOIN consolidations AS c ON c.epoch_start_date_time = s.snapshot_time
GLOBAL LEFT JOIN partial_withdrawals AS p ON p.epoch_start_date_time = s.snapshot_time
SETTINGS join_use_nulls = 1
