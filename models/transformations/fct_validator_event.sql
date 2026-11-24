---
table: fct_validator_event
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 15m"
  backfill: "@every 30s"
tags:
  - validator
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_block"
  - "{{external}}.canonical_beacon_block_deposit"
  - "{{external}}.canonical_beacon_block_execution_request_deposit"
  - "{{external}}.canonical_beacon_block_execution_request_withdrawal"
  - "{{external}}.canonical_beacon_block_execution_request_consolidation"
  - "{{external}}.canonical_beacon_block_bls_to_execution_change"
  - "{{external}}.canonical_beacon_block_voluntary_exit"
  - "{{external}}.canonical_beacon_block_proposer_slashing"
  - "{{external}}.canonical_beacon_block_attester_slashing"
  - "{{external}}.canonical_beacon_block_withdrawal"
  - "{{external}}.canonical_beacon_state_pending_deposit"
  - "{{external}}.canonical_beacon_state_pending_consolidation"
  - "{{external}}.canonical_beacon_state_pending_partial_withdrawal"
  - "{{external}}.canonical_beacon_validators"
  - "{{external}}.canonical_beacon_validators_pubkeys"
  - "{{external}}.canonical_beacon_validators_withdrawal_credentials"
---
-- Validator lifecycle events, one INSERT per kind of event. Only operations that
-- took effect are written. Where the chain can reject an operation (execution
-- requests, slashings of already slashed validators) the outcome is read from the
-- validator and queue state around the operation.
--
-- State snapshots: Cannon reads validators and queues at each epoch's first slot,
-- which is the state after that slot's block. So for an operation at slot s the
-- state before it is the snapshot of epoch ceil(s / 32) - 1 and the state after it
-- is the snapshot of epoch ceil(s / 32). Epochs are 384 seconds (12 second slots).
--
-- Operations that need the next snapshot are read again for one extra hour before
-- the interval, so ones whose snapshot arrived after the previous run are written
-- too. Rows are idempotent, and the version keeps rows with a known amount.
--
-- Deposits and top-ups. A deposit only gets a validator index when the validator
-- is created (immediately before Electra, when the deposit queue reaches it after).
-- Deposits are written for pubkeys deposited to in the interval that already have
-- an index, and for validators created in the interval (indices are assigned in
-- order), whenever their deposits were included. The deposit that created the
-- validator is a deposit, later ones are top-ups. Genesis validators were created
-- by deposits in the genesis state, which no block contains, so all of their
-- block deposits are top-ups. Otherwise the first deposit for the pubkey created
-- it (no first deposit was dropped for a bad signature on mainnet or hoodi up to
-- 2026-10-30). Xatu has no deposit index for bridge deposits, so when the first
-- block holds several bridge deposits for the pubkey their order is unknown and
-- all of them are marked as the deposit.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    new_validator_range AS (
        SELECT
            (
                SELECT max(`index`)
                FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
                WHERE meta_network_name = '{{ .env.NETWORK }}'
                  AND epoch_start_date_time = (
                    SELECT min(epoch_start_date_time)
                    FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
                    WHERE meta_network_name = '{{ .env.NETWORK }}'
                      AND `index` = 0
                      AND epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR AND fromUnixTimestamp({{ .bounds.start }})
                  )
            ) AS lo,
            (
                SELECT max(`index`)
                FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
                WHERE meta_network_name = '{{ .env.NETWORK }}'
                  AND epoch_start_date_time = (
                    SELECT max(epoch_start_date_time)
                    FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
                    WHERE meta_network_name = '{{ .env.NETWORK }}'
                      AND `index` = 0
                      AND epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
                  )
            ) AS hi
    ),
    new_validators AS (
        SELECT `index` AS validator_index, pubkey
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators_pubkeys" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND `index` > (SELECT coalesce(lo, hi) FROM new_validator_range)
          AND `index` <= (SELECT hi FROM new_validator_range)
    ),
    candidate_pubkeys AS (
        SELECT deposit_data_pubkey AS pubkey
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_deposit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        UNION ALL
        SELECT pubkey
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_deposit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        UNION ALL
        SELECT pubkey FROM new_validators
    ),
    -- Every deposit ever made to the candidate pubkeys. Bridge deposits come before
    -- deposit requests in a block. Xatu has no index for bridge deposits, so several
    -- bridge deposits to one pubkey in one block are ordered by their proof.
    deposits AS (
        SELECT
            slot, slot_start_date_time, epoch, epoch_start_date_time,
            toString(block_root) AS block_root,
            deposit_data_pubkey AS pubkey,
            toString(deposit_data_withdrawal_credentials) AS withdrawal_credentials,
            toUInt64(deposit_data_amount) AS amount,
            toUInt64(0) AS source_order,
            toUInt64(2147483647 + row_number() OVER (PARTITION BY block_root, deposit_data_pubkey ORDER BY deposit_proof)) AS event_position
        FROM (
            -- Deduplicated without FINAL, which would read every row's deposit proof.
            SELECT *
            FROM {{ index .dep "{{external}}" "canonical_beacon_block_deposit" "helpers" "from" }}
            PREWHERE deposit_data_pubkey GLOBAL IN (SELECT pubkey FROM candidate_pubkeys)
            WHERE meta_network_name = '{{ .env.NETWORK }}'
            LIMIT 1 BY block_root, deposit_data_pubkey, deposit_proof
        )
        UNION ALL
        SELECT
            slot, slot_start_date_time, epoch, epoch_start_date_time,
            toString(block_root) AS block_root,
            pubkey,
            toString(withdrawal_credentials) AS withdrawal_credentials,
            toUInt64(amount) AS amount,
            toUInt64(1) AS source_order,
            toUInt64(position_in_block) AS event_position
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_deposit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND pubkey GLOBAL IN (SELECT pubkey FROM candidate_pubkeys)
    ),
    ranked AS (
        SELECT
            *,
            dense_rank() OVER (PARTITION BY pubkey ORDER BY slot, source_order, if(source_order = 0, 0, event_position)) AS deposit_number
        FROM deposits
    ),
    genesis AS (
        SELECT count() AS validator_count
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time = fromUnixTimestamp({{ .env.GENESIS_TIMESTAMP }})
    ),
    indexed AS (
        SELECT DISTINCT pubkey, validator_index
        FROM (
            SELECT pubkey, validator_index
            FROM `{{ .self.database }}`.`dim_validator_pubkey` FINAL
            WHERE pubkey GLOBAL IN (SELECT pubkey FROM candidate_pubkeys)
            UNION ALL
            SELECT pubkey, validator_index FROM new_validators
        )
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + 4294967296 AS version,
    r.slot AS slot,
    r.slot_start_date_time AS slot_start_date_time,
    r.epoch AS epoch,
    r.epoch_start_date_time AS epoch_start_date_time,
    r.block_root AS block_root,
    i.validator_index AS validator_index,
    if(r.deposit_number = 1 AND i.validator_index >= (SELECT validator_count FROM genesis), 'deposit', 'top_up') AS event_type,
    r.event_position AS event_position,
    toNullable(r.amount) AS amount,
    CAST(NULL AS Nullable(UInt32)) AS counterparty_validator_index,
    toNullable(r.withdrawal_credentials) AS address
FROM ranked AS r
GLOBAL INNER JOIN indexed AS i ON r.pubkey = i.pubkey
WHERE r.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
   OR i.validator_index GLOBAL IN (SELECT validator_index FROM new_validators);

-- Withdrawals (builder withdrawals from Gloas on are not validator events). A
-- withdrawal is full when the validator is withdrawable at that epoch
-- (withdrawable_epoch <= epoch), otherwise partial. withdrawable_epoch is set once
-- an exit starts and never changes, and is set at least 256 epochs before the
-- validator becomes withdrawable, so the last snapshot in the interval has it for
-- every full withdrawal in the interval.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    snapshot AS (
        SELECT max(epoch_start_date_time) AS snapshot_time
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND `index` = 0
    ),
    withdrawals AS (
        SELECT
            slot, slot_start_date_time, epoch, epoch_start_date_time,
            toString(block_root) AS block_root,
            withdrawal_index,
            withdrawal_validator_index AS validator_index,
            toString(withdrawal_address) AS withdrawal_address,
            toUInt64(withdrawal_amount) AS amount
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_withdrawal" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND withdrawal_type != 'builder'
    ),
    withdrawable AS (
        SELECT `index` AS validator_index, withdrawable_epoch
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time = (SELECT snapshot_time FROM snapshot)
          AND withdrawable_epoch IS NOT NULL
          AND `index` GLOBAL IN (SELECT validator_index FROM withdrawals)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + 4294967296 AS version,
    w.slot AS slot,
    w.slot_start_date_time AS slot_start_date_time,
    w.epoch AS epoch,
    w.epoch_start_date_time AS epoch_start_date_time,
    w.block_root AS block_root,
    w.validator_index AS validator_index,
    if(w.epoch >= ifNull(we.withdrawable_epoch, 18446744073709551615), 'withdrawal_full', 'withdrawal_partial') AS event_type,
    toUInt64(w.withdrawal_index) AS event_position,
    toNullable(w.amount) AS amount,
    CAST(NULL AS Nullable(UInt32)) AS counterparty_validator_index,
    toNullable(w.withdrawal_address) AS address
FROM withdrawals AS w
GLOBAL LEFT JOIN withdrawable AS we ON w.validator_index = we.validator_index;

-- BLS to execution credential changes. Included changes are always applied.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + 4294967296 AS version,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    toString(block_root) AS block_root,
    exchanging_message_validator_index AS validator_index,
    'bls_to_execution_change' AS event_type,
    toUInt64(0) AS event_position,
    CAST(NULL AS Nullable(UInt64)) AS amount,
    CAST(NULL AS Nullable(UInt32)) AS counterparty_validator_index,
    toNullable(toString(exchanging_message_to_execution_address)) AS address
FROM {{ index .dep "{{external}}" "canonical_beacon_block_bls_to_execution_change" "helpers" "from" }} FINAL
WHERE meta_network_name = '{{ .env.NETWORK }}'
  AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }});

-- Voluntary exits. Included exits are always applied.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + 4294967296 AS version,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    toString(block_root) AS block_root,
    voluntary_exit_message_validator_index AS validator_index,
    'voluntary_exit' AS event_type,
    toUInt64(0) AS event_position,
    CAST(NULL AS Nullable(UInt64)) AS amount,
    CAST(NULL AS Nullable(UInt32)) AS counterparty_validator_index,
    CAST(NULL AS Nullable(String)) AS address
FROM {{ index .dep "{{external}}" "canonical_beacon_block_voluntary_exit" "helpers" "from" }} FINAL
WHERE meta_network_name = '{{ .env.NETWORK }}'
  AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }});

-- Slashings. A slashing names one validator (proposer slashing) or the validators
-- in both attestations (attester slashing), but only validators not yet slashed
-- are slashed. The event is written for the first operation naming the validator
-- between the snapshot where it is not slashed and the one where it is. The
-- counterparty is the proposer that included the slashing.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    ops AS (
        SELECT
            slot, slot_start_date_time, epoch, epoch_start_date_time,
            toString(block_root) AS block_root,
            0 AS op_order,
            signed_header_1_message_proposer_index AS validator_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_proposer_slashing" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR AND fromUnixTimestamp({{ .bounds.end }})
        UNION ALL
        SELECT
            slot, slot_start_date_time, epoch, epoch_start_date_time,
            toString(block_root) AS block_root,
            1 AS op_order,
            arrayJoin(arrayIntersect(attestation_1_attesting_indices, attestation_2_attesting_indices)) AS validator_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_attester_slashing" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR AND fromUnixTimestamp({{ .bounds.end }})
    ),
    first_ops AS (
        SELECT
            *,
            toUInt32(intDiv(slot + 31, 32)) AS after_epoch,
            epoch_start_date_time + toIntervalSecond(if(slot % 32 = 0, 0, 384)) AS after_epoch_start_date_time
        FROM ops
        ORDER BY slot, op_order
        LIMIT 1 BY validator_index, after_epoch
    ),
    state AS (
        SELECT epoch, `index` AS validator_index, slashed
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (
            SELECT after_epoch_start_date_time FROM first_ops
            UNION ALL
            SELECT after_epoch_start_date_time - INTERVAL 384 SECOND FROM first_ops
          )
          AND (epoch_start_date_time, `index`) GLOBAL IN (
            SELECT after_epoch_start_date_time, validator_index FROM first_ops
            UNION ALL
            SELECT after_epoch_start_date_time - INTERVAL 384 SECOND, validator_index FROM first_ops
          )
          AND `index` GLOBAL IN (SELECT validator_index FROM first_ops)
    ),
    proposers AS (
        SELECT toString(block_root) AS block_root, proposer_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR AND fromUnixTimestamp({{ .bounds.end }})
          AND toString(block_root) GLOBAL IN (SELECT block_root FROM first_ops)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + 4294967296 AS version,
    f.slot AS slot,
    f.slot_start_date_time AS slot_start_date_time,
    f.epoch AS epoch,
    f.epoch_start_date_time AS epoch_start_date_time,
    f.block_root AS block_root,
    f.validator_index AS validator_index,
    'slashed' AS event_type,
    toUInt64(0) AS event_position,
    CAST(NULL AS Nullable(UInt64)) AS amount,
    toNullable(p.proposer_index) AS counterparty_validator_index,
    CAST(NULL AS Nullable(String)) AS address
FROM first_ops AS f
GLOBAL INNER JOIN state AS before ON before.validator_index = f.validator_index AND before.epoch = f.after_epoch - 1
GLOBAL INNER JOIN state AS after ON after.validator_index = f.validator_index AND after.epoch = f.after_epoch
GLOBAL LEFT JOIN proposers AS p ON p.block_root = f.block_root
WHERE NOT before.slashed AND after.slashed;

-- Execution layer withdrawal requests (EIP-7002). The chain silently drops
-- invalid requests, so:
--   amount = 0 is a full exit request. It took effect when the validator has no
--   exit epoch before it and has one after it, the request's source address is the
--   validator's withdrawal address, and it is the first exit-starting operation
--   for the validator between the same two snapshots. A voluntary exit in a block
--   is always valid and is only valid when no exit has started, so when there is
--   one, it started the exit. Otherwise the first of these, in block processing
--   order, did: a slashing, an exit request, or a consolidation request whose pair
--   entered the consolidation queue. When there are several exit requests and the
--   validator had pending partial withdrawals (which make exit requests invalid
--   until they are paid out) the first valid one cannot be told, so none is
--   written.
--   amount > 0 is a partial withdrawal request. Accepted requests are appended to
--   the pending partial withdrawal queue, so the validator's entries in the
--   snapshot after the request that are not in the snapshot before it are the
--   accepted requests, in order (entries leave the queue only once withdrawable,
--   long after they are added). When every request for the validator between the
--   two snapshots was accepted they are matched in order and the amount is the
--   queued amount. When only some were, which ones cannot be told and none are
--   written.
-- Xatu stores an unset exit epoch as NULL in older data and as FAR_FUTURE_EPOCH
-- (2^64 - 1) in newer data, so both mean unset.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    requests AS (
        SELECT
            r.slot AS slot, r.slot_start_date_time AS slot_start_date_time,
            r.epoch AS epoch, r.epoch_start_date_time AS epoch_start_date_time,
            toString(r.block_root) AS block_root,
            r.position_in_block AS position_in_block,
            toString(r.source_address) AS source_address,
            r.validator_pubkey AS pubkey,
            r.amount AS requested_amount,
            toUInt32(intDiv(r.slot + 31, 32)) AS after_epoch,
            r.epoch_start_date_time + toIntervalSecond(if(r.slot % 32 = 0, 0, 384)) AS after_epoch_start_date_time
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_withdrawal" "helpers" "from" }} AS r FINAL
        WHERE r.meta_network_name = '{{ .env.NETWORK }}'
          AND r.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR AND fromUnixTimestamp({{ .bounds.end }})
    ),
    indexed AS (
        SELECT pubkey, validator_index
        FROM `{{ .self.database }}`.`dim_validator_pubkey` FINAL
        WHERE pubkey GLOBAL IN (SELECT pubkey FROM requests)
    ),
    addresses AS (
        -- 0x01 and 0x02 credentials of a validator share one address.
        SELECT `index` AS validator_index, concat('0x', substring(argMax(withdrawal_credentials, withdrawal_credentials), 27, 40)) AS withdrawal_address
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators_withdrawal_credentials" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND `index` GLOBAL IN (SELECT validator_index FROM indexed)
          AND substring(withdrawal_credentials, 1, 4) IN ('0x01', '0x02')
        GROUP BY `index`
    ),
    resolved AS (
        SELECT
            q.slot AS slot, q.slot_start_date_time AS slot_start_date_time,
            q.epoch AS epoch, q.epoch_start_date_time AS epoch_start_date_time,
            q.block_root AS block_root, q.position_in_block AS position_in_block,
            q.source_address AS source_address, q.requested_amount AS requested_amount,
            q.after_epoch AS after_epoch, q.after_epoch_start_date_time AS after_epoch_start_date_time,
            i.validator_index AS validator_index
        FROM requests AS q
        GLOBAL INNER JOIN indexed AS i ON q.pubkey = i.pubkey
        GLOBAL INNER JOIN addresses AS a ON a.validator_index = i.validator_index
        WHERE lower(q.source_address) = lower(a.withdrawal_address)
    ),
    voluntary_exits AS (
        SELECT voluntary_exit_message_validator_index AS validator_index, toUInt32(intDiv(slot + 31, 32)) AS after_epoch
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_voluntary_exit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR - INTERVAL 384 SECOND AND fromUnixTimestamp({{ .bounds.end }})
          AND voluntary_exit_message_validator_index GLOBAL IN (SELECT validator_index FROM resolved)
    ),
    consolidation_requests AS (
        SELECT
            s.validator_index AS source_index,
            t.validator_index AS target_index,
            c.slot AS slot,
            c.position_in_block AS position_in_block,
            toUInt32(intDiv(c.slot + 31, 32)) AS after_epoch,
            c.epoch_start_date_time + toIntervalSecond(if(c.slot % 32 = 0, 0, 384)) AS after_epoch_start_date_time
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_consolidation" "helpers" "from" }} AS c FINAL
        GLOBAL INNER JOIN (
            SELECT pubkey, `index` AS validator_index
            FROM {{ index .dep "{{external}}" "canonical_beacon_validators_pubkeys" "helpers" "from" }} FINAL
            WHERE meta_network_name = '{{ .env.NETWORK }}'
              AND `index` GLOBAL IN (SELECT validator_index FROM resolved)
        ) AS s ON c.source_pubkey = s.pubkey
        GLOBAL INNER JOIN (
            SELECT pubkey, validator_index
            FROM `{{ .self.database }}`.`dim_validator_pubkey` FINAL
            WHERE pubkey GLOBAL IN (
                SELECT target_pubkey
                FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_consolidation" "helpers" "from" }} FINAL
                WHERE meta_network_name = '{{ .env.NETWORK }}'
                  AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR - INTERVAL 384 SECOND AND fromUnixTimestamp({{ .bounds.end }})
              )
        ) AS t ON c.target_pubkey = t.pubkey
        WHERE c.meta_network_name = '{{ .env.NETWORK }}'
          AND c.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR - INTERVAL 384 SECOND AND fromUnixTimestamp({{ .bounds.end }})
          AND c.source_pubkey != c.target_pubkey
    ),
    accepted_consolidations AS (
        SELECT *
        FROM consolidation_requests
        WHERE (after_epoch, source_index, target_index) GLOBAL IN (
            SELECT epoch, source_index, target_index
            FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_consolidation" "helpers" "from" }} FINAL
            WHERE meta_network_name = '{{ .env.NETWORK }}'
              AND epoch_start_date_time GLOBAL IN (SELECT after_epoch_start_date_time FROM consolidation_requests)
        )
    ),
    -- The first exit request (by slot, then position) for each validator between
    -- two snapshots, and other exit-starting operations that came before it. In
    -- block processing order slashings come before execution requests, and
    -- withdrawal requests before consolidation requests.
    first_requests AS (
        SELECT validator_index, after_epoch, slot, position_in_block
        FROM (
            SELECT
                validator_index, after_epoch, slot, position_in_block,
                row_number() OVER (PARTITION BY validator_index, after_epoch ORDER BY slot, position_in_block) AS request_number
            FROM resolved
            WHERE requested_amount = 0
        )
        WHERE request_number = 1
    ),
    slashing_ops AS (
        SELECT signed_header_1_message_proposer_index AS validator_index, toUInt32(intDiv(slot + 31, 32)) AS after_epoch, slot
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_proposer_slashing" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR - INTERVAL 384 SECOND AND fromUnixTimestamp({{ .bounds.end }})
        UNION ALL
        SELECT arrayJoin(arrayIntersect(attestation_1_attesting_indices, attestation_2_attesting_indices)) AS validator_index, toUInt32(intDiv(slot + 31, 32)) AS after_epoch, slot
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_attester_slashing" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR - INTERVAL 384 SECOND AND fromUnixTimestamp({{ .bounds.end }})
    ),
    preceded AS (
        SELECT r.validator_index AS validator_index, r.after_epoch AS after_epoch
        FROM first_requests AS r
        GLOBAL INNER JOIN slashing_ops AS o ON o.validator_index = r.validator_index AND o.after_epoch = r.after_epoch
        WHERE o.slot <= r.slot
        UNION ALL
        SELECT r.validator_index AS validator_index, r.after_epoch AS after_epoch
        FROM first_requests AS r
        GLOBAL INNER JOIN accepted_consolidations AS c ON c.source_index = r.validator_index AND c.after_epoch = r.after_epoch
        WHERE c.slot < r.slot
    ),
    state AS (
        SELECT epoch, `index` AS validator_index, exit_epoch IS NOT NULL AND exit_epoch != 18446744073709551615 AS exit_started
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (
            SELECT after_epoch_start_date_time FROM resolved
            UNION ALL
            SELECT after_epoch_start_date_time - INTERVAL 384 SECOND FROM resolved
          )
          AND (epoch_start_date_time, `index`) GLOBAL IN (
            SELECT after_epoch_start_date_time, validator_index FROM resolved
            UNION ALL
            SELECT after_epoch_start_date_time - INTERVAL 384 SECOND, validator_index FROM resolved
          )
          AND `index` GLOBAL IN (SELECT validator_index FROM resolved)
    ),
    pending_partials_before AS (
        SELECT DISTINCT epoch + 1 AS after_epoch, validator_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_partial_withdrawal" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (SELECT after_epoch_start_date_time - INTERVAL 384 SECOND FROM resolved WHERE requested_amount = 0)
          AND validator_index GLOBAL IN (SELECT validator_index FROM resolved WHERE requested_amount = 0)
    ),
    ambiguous_exits AS (
        SELECT validator_index, after_epoch
        FROM resolved
        WHERE requested_amount = 0
        GROUP BY validator_index, after_epoch
        HAVING count() > 1
    ),
    exits AS (
        SELECT *
        FROM resolved
        WHERE requested_amount = 0
          AND (validator_index, after_epoch) GLOBAL NOT IN (SELECT validator_index, after_epoch FROM voluntary_exits)
          AND (validator_index, after_epoch) GLOBAL NOT IN (
            SELECT validator_index, after_epoch FROM ambiguous_exits
            WHERE (validator_index, after_epoch) GLOBAL IN (SELECT validator_index, after_epoch FROM pending_partials_before)
          )
          AND (validator_index, slot, position_in_block) GLOBAL IN (SELECT validator_index, slot, position_in_block FROM first_requests)
          AND (validator_index, after_epoch) GLOBAL NOT IN (SELECT validator_index, after_epoch FROM preceded)
          AND (validator_index, after_epoch) GLOBAL IN (
            SELECT after.validator_index, after.epoch
            FROM state AS after
            GLOBAL INNER JOIN state AS before ON before.validator_index = after.validator_index AND before.epoch = after.epoch - 1
            WHERE NOT before.exit_started AND after.exit_started
          )
    ),
    partial_requests AS (
        SELECT
            *,
            row_number() OVER (PARTITION BY validator_index, after_epoch ORDER BY slot, position_in_block) AS request_number,
            count() OVER (PARTITION BY validator_index, after_epoch) AS request_count
        FROM resolved
        WHERE requested_amount > 0
    ),
    queue_rows AS (
        SELECT epoch, validator_index, toUInt64(amount) AS amount, withdrawable_epoch, position_in_queue
        FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_partial_withdrawal" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (
            SELECT after_epoch_start_date_time FROM partial_requests
            UNION ALL
            SELECT after_epoch_start_date_time - INTERVAL 384 SECOND FROM partial_requests
          )
          AND validator_index GLOBAL IN (SELECT validator_index FROM partial_requests)
    ),
    new_entries AS (
        -- Entries after the requests beyond the number with the same amount and
        -- withdrawable epoch before them.
        SELECT
            a.after_epoch AS after_epoch,
            a.validator_index AS validator_index,
            a.amount AS amount,
            row_number() OVER (PARTITION BY a.after_epoch, a.validator_index ORDER BY a.position_in_queue) AS request_number,
            count() OVER (PARTITION BY a.after_epoch, a.validator_index) AS entry_count
        FROM (
            SELECT
                epoch AS after_epoch, validator_index, amount, withdrawable_epoch, position_in_queue,
                row_number() OVER (PARTITION BY epoch, validator_index, amount, withdrawable_epoch ORDER BY position_in_queue) AS key_number
            FROM queue_rows
        ) AS a
        GLOBAL LEFT JOIN (
            SELECT epoch + 1 AS after_epoch, validator_index, amount, withdrawable_epoch, count() AS before_count
            FROM queue_rows
            GROUP BY after_epoch, validator_index, amount, withdrawable_epoch
        ) AS b
            ON b.after_epoch = a.after_epoch AND b.validator_index = a.validator_index
           AND b.amount = a.amount AND b.withdrawable_epoch = a.withdrawable_epoch
        WHERE a.key_number > b.before_count
    ),
    partials AS (
        SELECT
            q.slot AS slot, q.slot_start_date_time AS slot_start_date_time,
            q.epoch AS epoch, q.epoch_start_date_time AS epoch_start_date_time,
            q.block_root AS block_root, q.position_in_block AS position_in_block,
            q.source_address AS source_address, q.validator_index AS validator_index,
            u.amount AS queued_amount
        FROM partial_requests AS q
        GLOBAL INNER JOIN new_entries AS u
            ON u.after_epoch = q.after_epoch
           AND u.validator_index = q.validator_index
           AND u.request_number = q.request_number
        WHERE u.entry_count = q.request_count
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + 4294967296 AS version,
    slot, slot_start_date_time, epoch, epoch_start_date_time, block_root,
    validator_index,
    'exit_request' AS event_type,
    toUInt64(position_in_block) AS event_position,
    CAST(NULL AS Nullable(UInt64)) AS amount,
    CAST(NULL AS Nullable(UInt32)) AS counterparty_validator_index,
    toNullable(source_address) AS address
FROM exits
UNION ALL
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + 4294967296 AS version,
    slot, slot_start_date_time, epoch, epoch_start_date_time, block_root,
    validator_index,
    'withdrawal_request' AS event_type,
    toUInt64(position_in_block) AS event_position,
    toNullable(queued_amount) AS amount,
    CAST(NULL AS Nullable(UInt32)) AS counterparty_validator_index,
    toNullable(source_address) AS address
FROM partials;

-- Execution layer consolidation requests (EIP-7251).
--   source = target is a switch to compounding (0x02) credentials. It applies when
--   the validator is active with no exit started before the request (status
--   active_ongoing), has 0x01 credentials at that time (0x01 from its deposit, or a
--   BLS change at or before the request's slot), the request's source address is
--   its withdrawal address, and no earlier switch applied. Its credentials are 0x02
--   today.
--   source != target is a consolidation. It applies when the (source, target)
--   pair is in the pending consolidation queue after the request and not before.
--   It is processed at the epoch the pair leaves the queue, moving
--   min(balance, effective_balance) of the source to the target (nothing if the
--   source was slashed). That balance is taken from the snapshot before
--   processing, less the source's withdrawals in the rest of that epoch, plus its
--   pending deposits applied at the same epoch transition.
-- Requests are read for the interval (plus the extra hour), and also for every pair
-- processed in the interval, so the moved amount is filled in when it is known.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    processed AS (
        -- Pairs in the queue at epoch P - 1 and gone at epoch P, for P in the interval.
        SELECT
            prev.epoch + 1 AS processed_epoch,
            prev.epoch_start_date_time AS prev_epoch_start_date_time,
            prev.source_index AS source_index,
            prev.target_index AS target_index
        FROM (
            SELECT epoch, epoch_start_date_time, source_index, target_index
            FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_consolidation" "helpers" "from" }} FINAL
            WHERE meta_network_name = '{{ .env.NETWORK }}'
              AND epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 384 SECOND AND fromUnixTimestamp({{ .bounds.end }}) - INTERVAL 384 SECOND
        ) AS prev
        GLOBAL LEFT ANTI JOIN (
            SELECT epoch, source_index, target_index
            FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_consolidation" "helpers" "from" }} FINAL
            WHERE meta_network_name = '{{ .env.NETWORK }}'
              AND epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        ) AS cur ON cur.epoch = prev.epoch + 1 AND cur.source_index = prev.source_index AND cur.target_index = prev.target_index
        -- The snapshot at P must exist (an empty queue writes no rows).
        WHERE prev.epoch_start_date_time + INTERVAL 384 SECOND GLOBAL IN (
            SELECT DISTINCT epoch_start_date_time
            FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }}
            WHERE meta_network_name = '{{ .env.NETWORK }}'
              AND epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
              AND `index` = 0
        )
    ),
    processed_pubkeys AS (
        SELECT `index` AS validator_index, pubkey
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators_pubkeys" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND (`index` GLOBAL IN (SELECT source_index FROM processed) OR `index` GLOBAL IN (SELECT target_index FROM processed))
    ),
    requests AS (
        SELECT
            slot, slot_start_date_time, epoch, epoch_start_date_time,
            toString(block_root) AS block_root,
            position_in_block,
            toString(source_address) AS source_address,
            source_pubkey,
            target_pubkey,
            toUInt32(intDiv(slot + 31, 32)) AS after_epoch,
            epoch_start_date_time + toIntervalSecond(if(slot % 32 = 0, 0, 384)) AS after_epoch_start_date_time
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_consolidation" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND (
            slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR AND fromUnixTimestamp({{ .bounds.end }})
            OR (source_pubkey != target_pubkey AND source_pubkey GLOBAL IN (SELECT pubkey FROM processed_pubkeys))
            OR (source_pubkey = target_pubkey AND source_pubkey GLOBAL IN (
                SELECT source_pubkey
                FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_request_consolidation" "helpers" "from" }} FINAL
                WHERE meta_network_name = '{{ .env.NETWORK }}'
                  AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR AND fromUnixTimestamp({{ .bounds.end }})
                  AND source_pubkey = target_pubkey
            ))
          )
    ),
    indexed AS (
        SELECT pubkey, validator_index
        FROM `{{ .self.database }}`.`dim_validator_pubkey` FINAL
        WHERE (pubkey GLOBAL IN (SELECT source_pubkey FROM requests) OR pubkey GLOBAL IN (SELECT target_pubkey FROM requests))
    ),
    resolved AS (
        SELECT
            q.slot AS slot, q.slot_start_date_time AS slot_start_date_time,
            q.epoch AS epoch, q.epoch_start_date_time AS epoch_start_date_time,
            q.block_root AS block_root, q.position_in_block AS position_in_block,
            q.source_address AS source_address,
            q.after_epoch AS after_epoch, q.after_epoch_start_date_time AS after_epoch_start_date_time,
            s.validator_index AS source_index, t.validator_index AS target_index
        FROM requests AS q
        GLOBAL INNER JOIN indexed AS s ON s.pubkey = q.source_pubkey
        GLOBAL INNER JOIN indexed AS t ON t.pubkey = q.target_pubkey
    ),

    -- Switches to compounding credentials.
    credentials AS (
        SELECT
            `index` AS validator_index,
            min(substring(withdrawal_credentials, 1, 4)) AS initial_type,
            max(substring(withdrawal_credentials, 1, 4)) AS current_type,
            concat('0x', substring(argMax(withdrawal_credentials, withdrawal_credentials), 27, 40)) AS withdrawal_address
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators_withdrawal_credentials" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND `index` GLOBAL IN (SELECT source_index FROM resolved WHERE source_index = target_index)
        GROUP BY `index`
    ),
    bls_changes AS (
        SELECT exchanging_message_validator_index AS validator_index, min(slot) AS bls_change_slot
        FROM {{ index .dep "{{external}}" "canonical_beacon_block_bls_to_execution_change" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND exchanging_message_validator_index GLOBAL IN (SELECT source_index FROM resolved WHERE source_index = target_index)
        GROUP BY validator_index
    ),
    switch_state AS (
        SELECT epoch, `index` AS validator_index, status
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (SELECT after_epoch_start_date_time - INTERVAL 384 SECOND FROM resolved WHERE source_index = target_index)
          AND (epoch_start_date_time, `index`) GLOBAL IN (SELECT after_epoch_start_date_time - INTERVAL 384 SECOND, source_index FROM resolved WHERE source_index = target_index)
          AND `index` GLOBAL IN (SELECT source_index FROM resolved WHERE source_index = target_index)
    ),
    switches AS (
        SELECT *
        FROM resolved
        WHERE (source_index, slot, position_in_block) GLOBAL IN (
            SELECT q.source_index, q.slot, q.position_in_block
            FROM resolved AS q
            GLOBAL INNER JOIN credentials AS c ON c.validator_index = q.source_index
            GLOBAL INNER JOIN switch_state AS s ON s.validator_index = q.source_index AND s.epoch = q.after_epoch - 1
            GLOBAL LEFT JOIN bls_changes AS b ON b.validator_index = q.source_index
            WHERE q.source_index = q.target_index
              AND c.current_type = '0x02'
              AND c.initial_type IN ('0x00', '0x01')
              AND (c.initial_type = '0x01' OR (b.bls_change_slot IS NOT NULL AND b.bls_change_slot <= q.slot))
              AND lower(q.source_address) = lower(c.withdrawal_address)
              AND s.status = 'active_ongoing'
        )
        ORDER BY slot, position_in_block
        LIMIT 1 BY source_index
    ),

    -- Consolidations.
    queue AS (
        SELECT epoch, source_index, target_index
        FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_consolidation" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (
            SELECT after_epoch_start_date_time FROM resolved WHERE source_index != target_index
            UNION ALL
            SELECT after_epoch_start_date_time - INTERVAL 384 SECOND FROM resolved WHERE source_index != target_index
          )
          AND source_index GLOBAL IN (SELECT source_index FROM resolved WHERE source_index != target_index)
    ),
    accepted AS (
        SELECT *
        FROM resolved
        WHERE source_index != target_index
          AND (after_epoch, source_index, target_index) GLOBAL IN (SELECT epoch, source_index, target_index FROM queue)
          AND (after_epoch - 1, source_index, target_index) GLOBAL NOT IN (SELECT epoch, source_index, target_index FROM queue)
        ORDER BY slot, position_in_block
        LIMIT 1 BY source_index, target_index, after_epoch
    ),
    processed_state AS (
        SELECT epoch, `index` AS validator_index, balance, effective_balance, slashed
        FROM {{ index .dep "{{external}}" "canonical_beacon_validators" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (SELECT prev_epoch_start_date_time FROM processed)
          AND (epoch_start_date_time, `index`) GLOBAL IN (SELECT prev_epoch_start_date_time, source_index FROM processed)
          AND `index` GLOBAL IN (SELECT source_index FROM processed)
    ),
    processed_withdrawals AS (
        SELECT p.processed_epoch AS processed_epoch, p.source_index AS source_index, sum(toUInt64(w.withdrawal_amount)) AS withdrawn
        FROM processed AS p
        GLOBAL INNER JOIN (
            SELECT slot, withdrawal_validator_index, withdrawal_amount
            FROM {{ index .dep "{{external}}" "canonical_beacon_block_withdrawal" "helpers" "from" }} FINAL
            WHERE meta_network_name = '{{ .env.NETWORK }}'
              AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 384 SECOND AND fromUnixTimestamp({{ .bounds.end }})
              AND withdrawal_validator_index GLOBAL IN (SELECT source_index FROM processed)
        ) AS w ON w.withdrawal_validator_index = p.source_index
        WHERE w.slot > (p.processed_epoch - 1) * 32 AND w.slot < p.processed_epoch * 32
        GROUP BY processed_epoch, source_index
    ),
    pending_deposits AS (
        SELECT epoch, pubkey, slot, toUInt64(amount) AS amount
        FROM {{ index .dep "{{external}}" "canonical_beacon_state_pending_deposit" "helpers" "from" }} FINAL
        WHERE meta_network_name = '{{ .env.NETWORK }}'
          AND epoch_start_date_time GLOBAL IN (
            SELECT prev_epoch_start_date_time FROM processed
            UNION ALL
            SELECT prev_epoch_start_date_time + INTERVAL 384 SECOND FROM processed
          )
          AND pubkey GLOBAL IN (SELECT pubkey FROM processed_pubkeys)
    ),
    processed_deposits AS (
        -- Deposits for the source in the queue before processing and gone after it.
        -- Deposits added during that epoch cannot be applied yet (they are not
        -- finalized), so the ones still queued with an older slot were postponed.
        SELECT
            p.processed_epoch AS processed_epoch,
            p.source_index AS source_index,
            toInt64(sumIf(d.amount, d.epoch = p.processed_epoch - 1))
              - toInt64(sumIf(d.amount, d.epoch = p.processed_epoch AND d.slot <= (p.processed_epoch - 1) * 32)) AS deposited
        FROM processed AS p
        GLOBAL INNER JOIN processed_pubkeys AS k ON k.validator_index = p.source_index
        GLOBAL INNER JOIN pending_deposits AS d ON d.pubkey = k.pubkey
        GROUP BY processed_epoch, source_index
    ),
    moved AS (
        SELECT
            p.source_index AS source_index,
            p.target_index AS target_index,
            p.processed_epoch AS processed_epoch,
            toUInt64(if(s.slashed, toInt64(0), greatest(toInt64(0), least(
                toInt64(ifNull(s.balance, 0)) - toInt64(ifNull(w.withdrawn, 0)) + toInt64(ifNull(d.deposited, 0)),
                toInt64(ifNull(s.effective_balance, 0))
            )))) AS amount
        FROM processed AS p
        GLOBAL INNER JOIN processed_state AS s ON s.validator_index = p.source_index AND s.epoch = p.processed_epoch - 1
        GLOBAL LEFT JOIN processed_withdrawals AS w ON w.source_index = p.source_index AND w.processed_epoch = p.processed_epoch
        GLOBAL LEFT JOIN processed_deposits AS d ON d.source_index = p.source_index AND d.processed_epoch = p.processed_epoch
    ),
    consolidations AS (
        -- A source can be consolidated only once, so the accepted request for a
        -- processed pair is its latest accepted request before processing.
        SELECT
            a.slot AS slot, a.slot_start_date_time AS slot_start_date_time,
            a.epoch AS epoch, a.epoch_start_date_time AS epoch_start_date_time,
            a.block_root AS block_root, a.position_in_block AS position_in_block,
            a.source_address AS source_address,
            a.source_index AS source_index, a.target_index AS target_index,
            if(m.processed_epoch > a.after_epoch, toNullable(toUInt64(m.amount)), CAST(NULL AS Nullable(UInt64))) AS moved_amount
        FROM accepted AS a
        GLOBAL LEFT JOIN moved AS m ON m.source_index = a.source_index AND m.target_index = a.target_index
    ),
    consolidation_sides AS (
        SELECT
            *,
            arrayJoin([(source_index, 'consolidation_source', target_index), (target_index, 'consolidation_target', source_index)]) AS side
        FROM consolidations
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + 4294967296 AS version,
    slot, slot_start_date_time, epoch, epoch_start_date_time, block_root,
    source_index AS validator_index,
    'switch_to_compounding' AS event_type,
    toUInt64(position_in_block) AS event_position,
    CAST(NULL AS Nullable(UInt64)) AS amount,
    CAST(NULL AS Nullable(UInt32)) AS counterparty_validator_index,
    toNullable(source_address) AS address
FROM switches
UNION ALL
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toUInt64({{ .task.start }}) + if(moved_amount IS NULL, 0, 4294967296) AS version,
    slot, slot_start_date_time, epoch, epoch_start_date_time, block_root,
    side.1 AS validator_index,
    side.2 AS event_type,
    toUInt64(position_in_block) AS event_position,
    moved_amount AS amount,
    toNullable(side.3) AS counterparty_validator_index,
    toNullable(source_address) AS address
FROM consolidation_sides
SETTINGS join_use_nulls = 1
