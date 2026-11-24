---
table: int_engine_new_payload_opcode_gram_hourly
type: incremental
interval:
  type: block
  max: 3600
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 15s"
tags:
  - hourly
  - execution
  - engine_api
  - new_payload
  - opcode
  - gas
dependencies:
  - "{{transformation}}.int_block_opcode_gas"
  - "{{transformation}}.int_execution_block_by_date"
  - "{{external}}.canonical_execution_block"
---
-- Hourly sufficient statistics for a per-client regression of block execution time on gas
-- per opcode group:
--
--   median engine_newPayload ms = b0 + b1 * txs/100 + b2 * non-opcode Mgas + sum_g bg * Mgas_g
--
-- For each hour and execution client version on reference nodes, stores X'X, X'y and y'y.
-- Summing these over any set of hours gives the normal equations for that set, so
-- fct_opcode_cost_by_el_client_daily and fct_opcode_cost_by_el_client_version can fit days and
-- releases (and bootstrap them by resampling hours) without rereading blocks.
--
-- Only reference nodes (node_class eip7870-block-builder) are used: they share hardware, so
-- duration differences come from the client rather than the machine.
--
-- Outliers are dropped per hour and client before the sums: blocks above the hour's P99 duration
-- (stalls, GC pauses) and blocks above 3x the hour's median ms per Mgas (near-empty blocks whose
-- time is all fixed cost).
--
-- Like the other hourly execution models, the block range is widened to whole hours so the hour
-- at the head is rebuilt with complete data on the next run.
--
-- fct_engine_new_payload_by_el_client has slot intervals, so it can't be a declared dependency of
-- this block-interval model. It is read directly, and this first statement fails the task (CBT
-- retries it) until that table has reached the last block in the range.
SELECT throwIf(
    (
        SELECT max(slot_start_date_time)
        FROM `{{ .self.database }}`.`fct_engine_new_payload_by_el_client`
    ) < (
        SELECT max(block_date_time)
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    ),
    'fct_engine_new_payload_by_el_client has not reached the end of this block range yet'
);

INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    ['SLOAD', 'SSTORE', 'CALL', 'STATICCALL', 'DELEGATECALL', 'KECCAK256', 'EXTCODESIZE', 'EXTCODEHASH',
     'EXTCODECOPY', 'BALANCE', 'CREATE2', 'CREATE', 'SELFDESTRUCT', 'MCOPY', 'CALLDATACOPY', 'CODECOPY',
     'RETURNDATACOPY', 'EXP', 'TLOAD', 'TSTORE', 'MULMOD', 'ADDMOD', 'MUL', 'DIV', 'SDIV', 'MOD',
     'LOG', 'ARITHMETIC', 'CONTROL', 'ENVIRONMENT', 'MEMORY', 'STACK'] AS opcode_groups,
    arrayConcat(['INTERCEPT', 'TX_COUNT', 'NON_OPCODE_GAS'], opcode_groups) AS features,
    hour_bounds AS (
        SELECT
            toStartOfHour(min(block_date_time)) AS min_hour,
            toStartOfHour(max(block_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    ),
    blocks AS (
        SELECT
            block_number,
            block_date_time
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds)
          AND block_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    block_range AS (
        SELECT
            min(block_number) AS min_block,
            max(block_number) AS max_block
        FROM blocks
    ),
    block_hashes AS (
        SELECT
            block_number,
            block_hash
        FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN (SELECT min_block FROM block_range) AND (SELECT max_block FROM block_range)
            AND meta_network_name = '{{ .env.NETWORK }}'
    ),
    -- Gas and executions per opcode group for each block, as arrays aligned with opcode_groups
    block_opcodes AS (
        SELECT
            block_number,
            sumForEach(arrayMap(i -> if(i = group_index, toFloat64(gas) / 1e6, 0.), range(1, length(opcode_groups) + 1))) AS group_mgas,
            sumForEach(arrayMap(i -> if(i = group_index, toFloat64(count), 0.), range(1, length(opcode_groups) + 1))) AS group_count
        FROM (
            SELECT
                block_number,
                gas,
                count,
                multiIf(
                    indexOf(opcode_groups, opcode) BETWEEN 1 AND 26, indexOf(opcode_groups, opcode),
                    opcode = 'CALLCODE', indexOf(opcode_groups, 'CALL'),
                    opcode = 'SHA3', indexOf(opcode_groups, 'KECCAK256'),
                    startsWith(opcode, 'LOG'), indexOf(opcode_groups, 'LOG'),
                    opcode IN ('ADD', 'SUB', 'LT', 'GT', 'SLT', 'SGT', 'EQ', 'ISZERO', 'AND', 'OR', 'XOR', 'NOT',
                               'BYTE', 'SHL', 'SHR', 'SAR', 'SIGNEXTEND', 'SMOD', 'CLZ'), indexOf(opcode_groups, 'ARITHMETIC'),
                    opcode IN ('JUMP', 'JUMPI', 'JUMPDEST', 'PC', 'GAS', 'STOP', 'RETURN', 'REVERT', 'INVALID'), indexOf(opcode_groups, 'CONTROL'),
                    opcode IN ('MLOAD', 'MSTORE', 'MSTORE8', 'MSIZE'), indexOf(opcode_groups, 'MEMORY'),
                    startsWith(opcode, 'PUSH') OR startsWith(opcode, 'DUP') OR startsWith(opcode, 'SWAP') OR opcode = 'POP', indexOf(opcode_groups, 'STACK'),
                    indexOf(opcode_groups, 'ENVIRONMENT')
                ) AS group_index
            FROM {{ index .dep "{{transformation}}" "int_block_opcode_gas" "helpers" "from" }} FINAL
            WHERE block_number BETWEEN (SELECT min_block FROM block_range) AND (SELECT max_block FROM block_range)
        )
        GROUP BY block_number
    ),
    payloads AS (
        SELECT
            block_hash,
            node_class,
            meta_execution_implementation,
            meta_execution_version,
            toFloat64(median_duration_ms) AS duration_ms,
            gas_used,
            tx_count
        FROM `{{ .self.database }}`.`fct_engine_new_payload_by_el_client` FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds) - INTERVAL 1 MINUTE
            AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 61 MINUTE
            AND status = 'VALID'
            AND node_class = 'eip7870-block-builder'
            AND median_duration_ms > 0
            AND gas_used > 0
    ),
    observations AS (
        SELECT
            toStartOfHour(b.block_date_time) AS hour_start_date_time,
            b.block_number AS block_number,
            p.node_class AS node_class,
            p.meta_execution_implementation AS meta_execution_implementation,
            p.meta_execution_version AS meta_execution_version,
            p.duration_ms AS duration_ms,
            p.duration_ms / (toFloat64(p.gas_used) / 1e6) AS ms_per_mgas,
            arrayConcat(
                [1., toFloat64(p.tx_count) / 100, toFloat64(p.gas_used) / 1e6 - arraySum(o.group_mgas)],
                o.group_mgas
            ) AS x,
            o.group_count AS group_count
        FROM payloads p
        GLOBAL INNER JOIN block_hashes h ON p.block_hash = h.block_hash
        GLOBAL INNER JOIN block_opcodes o ON h.block_number = o.block_number
        GLOBAL INNER JOIN blocks b ON h.block_number = b.block_number
    ),
    flagged AS (
        SELECT
            *,
            duration_ms <= quantile(0.99)(duration_ms) OVER w
                AND ms_per_mgas <= 3 * quantile(0.5)(ms_per_mgas) OVER w AS keep
        FROM observations
        WINDOW w AS (PARTITION BY hour_start_date_time, node_class, meta_execution_implementation)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    node_class,
    meta_execution_implementation,
    meta_execution_version,
    toUInt32(countIf(keep)) AS block_count,
    toUInt32(countIf(NOT keep)) AS trimmed_block_count,
    min(block_number) AS min_block_number,
    max(block_number) AS max_block_number,
    any(features) AS feature_names,
    sumForEachIf(
        arrayFlatten(arrayMap(i -> arrayMap(j -> x[i] * x[j], range(i, length(x) + 1)), range(1, length(x) + 1))),
        keep
    ) AS xtx,
    sumForEachIf(arrayMap(v -> v * duration_ms, x), keep) AS xty,
    sumIf(duration_ms * duration_ms, keep) AS yty,
    arrayMap(v -> toUInt64(v), sumForEachIf(group_count, keep)) AS opcode_count
FROM flagged
GROUP BY
    hour_start_date_time,
    node_class,
    meta_execution_implementation,
    meta_execution_version
HAVING block_count > 0
