---
table: fct_validator_operation_weekly
type: incremental
interval:
  type: slot
  max: 604800
fill:
  buffer: 3600
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - validator
dependencies:
  - "{{transformation}}.fct_validator_event"
  - "{{external}}.canonical_beacon_block_deposit"
  - "{{external}}.canonical_beacon_block_execution_request_deposit"
  - "{{external}}.canonical_beacon_validators"
---
-- Weekly counts and Gwei amounts of validator operations that took effect, by the
-- week (Monday to Sunday, UTC) of the block that included them, computed the same
-- way as fct_validator_operation_daily. validator_count is distinct over the
-- week. Each run rewrites every week it touches, with a row for every operation
-- (zero when there were none).
--
-- Deposits are counted from the deposit tables, by the week they were included,
-- because fct_validator_event can only write a deposit once its validator has an
-- index. A deposit is the first deposit for a pubkey that was not a genesis
-- validator (all deposits in that block when the first block holds several bridge
-- deposits for the pubkey), others are top-ups. validator_count is the number of
-- distinct pubkeys.
--
-- Everything else comes from fct_validator_event. The model stays an hour behind
-- it (fill.buffer), since an operation whose outcome is read from the next epoch's
-- state can be written there up to one run later. Consolidations are counted once
-- (by source) with no amount, since the moved balance is only known when the
-- queue processes them.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    weeks AS (
        SELECT toMonday(toDate(fromUnixTimestamp({{ .bounds.start }}))) + number * 7 AS week_start_date
        FROM numbers(toUInt64(intDiv(dateDiff('day', toMonday(toDate(fromUnixTimestamp({{ .bounds.start }}))), toMonday(toDate(fromUnixTimestamp({{ .bounds.end }})))), 7) + 1))
    ),
    operations AS (
        SELECT arrayJoin([
            'deposit', 'top_up', 'withdrawal_partial', 'withdrawal_full', 'bls_to_execution_change',
            'switch_to_compounding', 'consolidation', 'withdrawal_request', 'exit_request',
            'voluntary_exit', 'slashing'
        ]) AS operation
    ),
    week_deposits AS (
        SELECT deposit_data_pubkey AS pubkey
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_deposit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time >= toDateTime(toMonday(toDate(fromUnixTimestamp({{ .bounds.start }}))))
          AND slot_start_date_time < toDateTime(toMonday(toDate(fromUnixTimestamp({{ .bounds.end }})))) + INTERVAL 7 DAY
        UNION ALL
        SELECT pubkey
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_deposit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time >= toDateTime(toMonday(toDate(fromUnixTimestamp({{ .bounds.start }}))))
          AND slot_start_date_time < toDateTime(toMonday(toDate(fromUnixTimestamp({{ .bounds.end }})))) + INTERVAL 7 DAY
    ),
    deposits AS (
        SELECT
            slot, slot_start_date_time,
            deposit_data_pubkey AS pubkey,
            toUInt64(deposit_data_amount) AS amount,
            toUInt64(0) AS source_order,
            toUInt64(0) AS position
        FROM (
            -- Deduplicated without FINAL, which would read every row's deposit proof.
            SELECT *
            FROM {{ index .dep "{{external}}" "canonical_beacon_block_deposit" "helpers" "from" }}
            PREWHERE deposit_data_pubkey GLOBAL IN (SELECT pubkey FROM week_deposits)
            WHERE meta_network_name = '{{ .env.NETWORK }}'
            LIMIT 1 BY block_root, deposit_data_pubkey, deposit_proof
        )
        UNION ALL
        SELECT
            slot, slot_start_date_time,
            pubkey,
            toUInt64(amount) AS amount,
            toUInt64(1) AS source_order,
            toUInt64(position_in_block) AS position
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_deposit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND pubkey GLOBAL IN (SELECT pubkey FROM week_deposits)
    ),
    genesis_pubkeys AS (
        -- Genesis validators were created by deposits in the genesis state, which no
        -- block contains, so all of their block deposits are top-ups.
        SELECT pubkey
        FROM `{{ .self.database }}`.`dim_validator_pubkey` FINAL
        WHERE pubkey GLOBAL IN (SELECT pubkey FROM week_deposits)
          AND validator_index < (
            SELECT count()
            FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
            WHERE meta_network_name = '{{ .env.NETWORK }}'
              AND epoch_start_date_time = fromUnixTimestamp({{ .env.GENESIS_TIMESTAMP }})
          )
    ),
    classified AS (
        SELECT
            toMonday(toDate(slot_start_date_time)) AS week_start_date,
            pubkey,
            amount,
            if(
                dense_rank() OVER (PARTITION BY pubkey ORDER BY slot, source_order, position) = 1
                AND pubkey GLOBAL NOT IN (SELECT pubkey FROM genesis_pubkeys),
                'deposit', 'top_up'
            ) AS operation
        FROM deposits
    ),
    deposit_counts AS (
        SELECT
            week_start_date,
            operation,
            count() AS operation_count,
            uniqExact(pubkey) AS validator_count,
            toNullable(sum(amount)) AS amount
        FROM classified
        WHERE week_start_date GLOBAL IN (SELECT week_start_date FROM weeks)
        GROUP BY week_start_date, operation
    ),
    event_counts AS (
        SELECT
            toMonday(toDate(slot_start_date_time)) AS week_start_date,
            multiIf(event_type = 'consolidation_source', 'consolidation', event_type = 'slashed', 'slashing', event_type) AS operation,
            count() AS operation_count,
            uniqExact(validator_index) AS validator_count,
            if(operation IN ('withdrawal_partial', 'withdrawal_full', 'withdrawal_request'), toNullable(sum(ifNull(amount, 0))), CAST(NULL AS Nullable(UInt64))) AS amount
        FROM {{ index .dep "{{transformation}}" "fct_validator_event" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= toDateTime(toMonday(toDate(fromUnixTimestamp({{ .bounds.start }}))))
          AND slot_start_date_time < toDateTime(toMonday(toDate(fromUnixTimestamp({{ .bounds.end }})))) + INTERVAL 7 DAY
          AND event_type NOT IN ('deposit', 'top_up', 'consolidation_target')
        GROUP BY week_start_date, operation
    ),
    counts AS (
        SELECT * FROM deposit_counts
        UNION ALL
        SELECT * FROM event_counts
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    d.week_start_date AS week_start_date,
    o.operation AS operation,
    toUInt32(ifNull(c.operation_count, 0)) AS operation_count,
    toUInt32(ifNull(c.validator_count, 0)) AS validator_count,
    if(o.operation IN ('deposit', 'top_up', 'withdrawal_partial', 'withdrawal_full', 'withdrawal_request'),
       toNullable(toUInt64(ifNull(c.amount, 0))), CAST(NULL AS Nullable(UInt64))) AS amount
FROM weeks AS d
CROSS JOIN operations AS o
GLOBAL LEFT JOIN counts AS c ON c.week_start_date = d.week_start_date AND c.operation = o.operation
SETTINGS join_use_nulls = 1
