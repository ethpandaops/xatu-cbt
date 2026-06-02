---
table: int_execution_state_size_by_block
type: incremental
interval:
  type: block
  max: 10000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1m"
tags:
  - execution
  - state_size
  - cumulative
dependencies:
  - "{{external}}.execution_state_size_delta"
---
-- Absolute execution-layer state size per block, built by cumulatively summing the
-- per-block net deltas from execution_state_size_delta (gap-free from genesis).
--
-- Cumulative correctness needs sequential forward-fill (direction: tail,
-- allow_gap_skipping: false): each chunk seeds from the previous chunk's last block
-- (read back from this model's own output) and adds a running sum over the new range.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Seed: cumulative totals at the last block before this chunk. Reverse PK scan with
-- LIMIT 1 early-stops after one block, and the genesis chunk finds none, so the
-- scalar subqueries below resolve to NULL and COALESCE to 0.
prev_state AS (
    SELECT
        accounts,
        account_bytes,
        account_trienodes,
        account_trienode_bytes,
        contract_codes,
        contract_code_bytes,
        storages,
        storage_bytes,
        storage_trienodes,
        storage_trienode_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE block_number < {{ .bounds.start }}
    ORDER BY block_number DESC
    LIMIT 1
),
-- Collapse the multiple reporting clients down to one coherent row per block
-- (latest write wins) so the running sum is not multiplied by the client count.
block_deltas AS (
    SELECT
        block_number,
        account_delta,
        account_bytes_delta,
        account_trienode_delta,
        account_trienode_bytes_delta,
        contract_code_delta,
        contract_code_bytes_delta,
        storage_delta,
        storage_bytes_delta,
        storage_trienode_delta,
        storage_trienode_bytes_delta
    FROM {{ index .dep "{{external}}" "execution_state_size_delta" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
    ORDER BY block_number, updated_date_time DESC
    LIMIT 1 BY block_number
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    d.block_number,
    toUInt64(toInt64(COALESCE((SELECT accounts               FROM prev_state), 0)) + SUM(d.account_delta)               OVER w) AS accounts,
    toUInt64(toInt64(COALESCE((SELECT account_bytes          FROM prev_state), 0)) + SUM(d.account_bytes_delta)          OVER w) AS account_bytes,
    toUInt64(toInt64(COALESCE((SELECT account_trienodes      FROM prev_state), 0)) + SUM(d.account_trienode_delta)       OVER w) AS account_trienodes,
    toUInt64(toInt64(COALESCE((SELECT account_trienode_bytes FROM prev_state), 0)) + SUM(d.account_trienode_bytes_delta) OVER w) AS account_trienode_bytes,
    toUInt64(toInt64(COALESCE((SELECT contract_codes         FROM prev_state), 0)) + SUM(d.contract_code_delta)          OVER w) AS contract_codes,
    toUInt64(toInt64(COALESCE((SELECT contract_code_bytes    FROM prev_state), 0)) + SUM(d.contract_code_bytes_delta)    OVER w) AS contract_code_bytes,
    toUInt64(toInt64(COALESCE((SELECT storages               FROM prev_state), 0)) + SUM(d.storage_delta)                OVER w) AS storages,
    toUInt64(toInt64(COALESCE((SELECT storage_bytes          FROM prev_state), 0)) + SUM(d.storage_bytes_delta)          OVER w) AS storage_bytes,
    toUInt64(toInt64(COALESCE((SELECT storage_trienodes      FROM prev_state), 0)) + SUM(d.storage_trienode_delta)       OVER w) AS storage_trienodes,
    toUInt64(toInt64(COALESCE((SELECT storage_trienode_bytes FROM prev_state), 0)) + SUM(d.storage_trienode_bytes_delta) OVER w) AS storage_trienode_bytes
FROM block_deltas AS d
WINDOW w AS (ORDER BY d.block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY d.block_number
SETTINGS
    join_algorithm = 'hash',
    max_threads = 4;
