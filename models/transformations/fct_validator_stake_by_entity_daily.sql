---
table: fct_validator_stake_by_entity_daily
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
  - entity
  - credential
dependencies:
  - "{{external}}.canonical_beacon_validators"
  - "{{external}}.canonical_beacon_validators_withdrawal_credentials"
  - "{{external}}.canonical_beacon_block_bls_to_execution_change"
  - "{{external}}.canonical_beacon_block_execution_request_consolidation"
  - "{{transformation}}.dim_node"
---
-- Active validators and stake by entity and withdrawal credential type, from one
-- snapshot per day (the last epoch of the day, or the latest epoch for today).
-- Entities come from dim_node (cartographoor and ethseer labels) as they are
-- today, and validators without a label are 'unknown'. Each run rewrites every
-- day it touches. Keys stored for those days that the rewrite no longer produces
-- are written as zero.
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
    -- Credential type of each validator at each snapshot. Credentials only change
    -- 0x00 -> 0x01 (a BLS change, applied in its block) and 0x01 -> 0x02 (a switch
    -- request). canonical_beacon_validators_withdrawal_credentials keeps every
    -- distinct credential a validator had, so the initial type is the lowest one.
    -- For a validator with 0x02 credentials today that started below 0x02, the
    -- switch is its earliest switch request (source = target) from its withdrawal
    -- address, after its BLS change if it started at 0x00, at or after its
    -- activation epoch. Earlier requests cannot have applied (the validator was
    -- not active, or still had 0x00 credentials), and the earliest one passing
    -- these checks did apply: the only other reason to reject it is an exit having
    -- started, after which the validator could never have switched.
    credentials AS (
        SELECT
            `index` AS validator_index,
            min(substring(withdrawal_credentials, 1, 4)) AS initial_type,
            max(substring(withdrawal_credentials, 1, 4)) AS current_type,
            concat('0x', substring(argMax(withdrawal_credentials, withdrawal_credentials), 27, 40)) AS withdrawal_address
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators_withdrawal_credentials" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY `index`
    ),
    bls_changes AS (
        SELECT exchanging_message_validator_index AS validator_index, min(slot) AS bls_change_slot
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_bls_to_execution_change" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY validator_index
    ),
    switch_requests AS (
        SELECT
            k.validator_index AS validator_index,
            r.slot AS slot,
            toString(r.source_address) AS source_address
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_consolidation" "helpers" "from" }} AS r FINAL
        GLOBAL INNER JOIN (
            SELECT pubkey, validator_index
            FROM `{{ .self.database }}`.`dim_validator_pubkey` FINAL
            WHERE pubkey GLOBAL IN (
                SELECT source_pubkey
                FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_consolidation" "helpers" "from" }} FINAL
                WHERE meta_network_name = '{{ .env.NETWORK }}'
                  AND source_pubkey = target_pubkey
              )
        ) AS k ON k.pubkey = r.source_pubkey
        WHERE r.meta_network_name = '{{ .env.NETWORK }}'
          AND r.source_pubkey = r.target_pubkey
    ),
    activation AS (
        -- Older Xatu data stores activation epoch 0 as NULL, so NULL for a validator
        -- that has activated means 0. FAR_FUTURE_EPOCH means not activated.
        SELECT
            `index` AS validator_index,
            multiIf(activation_epoch = 18446744073709551615, NULL, activation_epoch IS NULL AND status NOT IN ('pending_initialized', 'pending_queued'), toNullable(toUInt64(0)), activation_epoch) AS activation_epoch
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time = (SELECT max(snapshot_time) FROM snapshots)
          AND `index` GLOBAL IN (SELECT validator_index FROM switch_requests)
    ),
    switches AS (
        SELECT q.validator_index AS validator_index, min(q.slot) AS switch_slot
        FROM switch_requests AS q
        GLOBAL INNER JOIN credentials AS c ON c.validator_index = q.validator_index
        GLOBAL INNER JOIN activation AS a ON a.validator_index = q.validator_index
        GLOBAL LEFT JOIN bls_changes AS b ON b.validator_index = q.validator_index
        WHERE c.current_type = '0x02'
          AND c.initial_type IN ('0x00', '0x01')
          AND (c.initial_type = '0x01' OR (b.bls_change_slot IS NOT NULL AND b.bls_change_slot <= q.slot))
          AND lower(q.source_address) = lower(c.withdrawal_address)
          AND a.activation_epoch IS NOT NULL
          AND a.activation_epoch <= intDiv(q.slot, 32)
        GROUP BY q.validator_index
    ),
    credential_changes AS (
        SELECT
            c.validator_index AS validator_index,
            c.initial_type AS initial_type,
            b.bls_change_slot AS bls_change_slot,
            s.switch_slot AS switch_slot
        FROM credentials AS c
        GLOBAL LEFT JOIN bls_changes AS b ON b.validator_index = c.validator_index
        GLOBAL LEFT JOIN switches AS s ON s.validator_index = c.validator_index
    ),
    validator_states AS (
        -- The snapshot of epoch X is the state after the block at slot X * 32.
        SELECT
            toDate(v.epoch_start_date_time) AS day_start_date,
            v.epoch AS epoch,
            v.epoch_start_date_time AS epoch_start_date_time,
            v.`index` AS validator_index,
            v.status AS status,
            v.effective_balance AS effective_balance,
            v.balance AS balance,
            multiIf(
                t.initial_type = '0x02' OR (t.switch_slot IS NOT NULL AND t.switch_slot <= v.epoch * 32), '0x02',
                t.initial_type = '0x01' OR (t.bls_change_slot IS NOT NULL AND t.bls_change_slot <= v.epoch * 32), '0x01',
                t.initial_type
            ) AS credential_type
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} AS v FINAL
        GLOBAL LEFT JOIN credential_changes AS t ON t.validator_index = v.`index`
        WHERE v.meta_network_name = '{{ .env.NETWORK }}'
          AND v.epoch_start_date_time GLOBAL IN (SELECT snapshot_time FROM snapshots)
    ),
    entities AS (
        SELECT validator_index, source
        FROM {{ index .dep "{{transformation}}" "dim_node" "helpers" "from" }} FINAL
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    max(epoch) AS epoch,
    max(epoch_start_date_time) AS epoch_start_date_time,
    entity,
    credential_type,
    toUInt32(sum(validators)) AS active_validator_count,
    toUInt64(sum(effective_balance)) AS active_effective_balance,
    toUInt64(sum(balance)) AS active_balance
FROM (
    SELECT
        v.day_start_date AS day_start_date,
        v.epoch AS epoch,
        v.epoch_start_date_time AS epoch_start_date_time,
        if(e.source IS NULL OR e.source = '', 'unknown', e.source) AS entity,
        v.credential_type AS credential_type,
        count() AS validators,
        sum(ifNull(v.effective_balance, 0)) AS effective_balance,
        sum(ifNull(v.balance, 0)) AS balance
    FROM validator_states AS v
    GLOBAL LEFT JOIN entities AS e ON e.validator_index = v.validator_index
    WHERE v.status IN ('active_ongoing', 'active_exiting', 'active_slashed')
    GROUP BY day_start_date, epoch, epoch_start_date_time, entity, credential_type

    UNION ALL

    SELECT
        s.day_start_date AS day_start_date,
        s.snapshot_epoch AS epoch,
        s.snapshot_time AS epoch_start_date_time,
        o.entity AS entity,
        o.credential_type AS credential_type,
        0 AS validators,
        0 AS effective_balance,
        0 AS balance
    FROM `{{ .self.database }}`.`{{ .self.table }}` AS o FINAL
    GLOBAL INNER JOIN snapshots AS s ON s.day_start_date = o.day_start_date
)
GROUP BY day_start_date, entity, credential_type
SETTINGS join_use_nulls = 1, parallel_distributed_insert_select = 0
