---
table: int_transaction_receipt_size
type: incremental
interval:
  type: block
  max: 10000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 10s"
  backfill: "@every 30s"
tags:
  - execution
  - receipt
  - size
dependencies:
  - "{{external}}.canonical_execution_transaction"
  - "{{external}}.canonical_execution_logs"
---
-- Computes RLP-encoded receipt size per transaction from canonical tx + log data.
--
-- =============================================================================
-- RLP ENCODING MODEL
-- =============================================================================
--
-- An Ethereum transaction receipt is RLP-encoded as:
--
--   For typed transactions (type > 0):
--     type_byte || RLP([status, cumulative_gas_used, logs_bloom, [logs...]])
--
--   For legacy transactions (type = 0):
--     RLP([status, cumulative_gas_used, logs_bloom, [logs...]])
--
-- Each log is RLP-encoded as:
--     RLP([address, [topic0, topic1, ...], data])
--
-- This query computes receipt bytes using ClickHouse lambdas:
--   - len_of_len: byte length of an integer for RLP length prefixes
--   - rlp_list_size: total size of an RLP list including its header
--   - rlp_bytes_size: total size of an RLP byte string including its header
--   - rlp_u64_size: total size of an RLP-encoded unsigned integer
--   - hex_bytes: convert hex string length to byte count
--
-- Notes and assumptions:
--   - Uses `success` as status (0/1) for the first receipt field.
--   - Uses cumulative gas from running sum(gas_used) by block/tx index.
--   - Normalizes nullable legacy transaction_type to 0 (no typed prefix byte).
--   - Uses the fixed RLP size of logsBloom (256-byte bloom => 259 bytes total).
--
-- =============================================================================
-- DATA SOURCES
-- =============================================================================
--
-- canonical_execution_logs: Per-log detail (address, topics, data) for RLP size
-- canonical_execution_transaction: Transaction list with type, status, gas_used
--   for cumulative gas computation and type prefix handling
--
-- =============================================================================
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- RLP encoding helper lambdas
    -- Byte length needed to represent integer n in big-endian
    (n) -> multiIf(
        n <= 255, 1,
        n <= 65535, 2,
        n <= 16777215, 3,
        n <= 4294967295, 4,
        n <= 1099511627775, 5,
        n <= 281474976710655, 6,
        n <= 72057594037927935, 7,
        8
    ) AS len_of_len,

    -- Total size of an RLP list with payload of p bytes (header + payload)
    (p) -> if(p <= 55, p + 1, p + 1 + len_of_len(p)) AS rlp_list_size,

    -- Byte count from a hex string (handles optional 0x prefix)
    (s) -> toUInt64(
        if(length(s) = 0, 0,
            if(startsWith(s, '0x'),
                intDiv(toInt64(length(s)) - 2, 2),
                intDiv(toInt64(length(s)), 2)))
    ) AS hex_bytes,

    -- Total size of an RLP byte string of b bytes
    -- Special case: single byte < 128 is encoded as itself (no header)
    (b, first_byte) -> if(
        b = 0, 1,
        if(b = 1 AND first_byte < 128, 1,
            if(b <= 55, b + 1, b + 1 + len_of_len(b)))
    ) AS rlp_bytes_size,

    -- Byte length of unsigned integer v in big-endian
    (v) -> multiIf(
        v = 0, 0,
        v <= 255, 1,
        v <= 65535, 2,
        v <= 16777215, 3,
        v <= 4294967295, 4,
        v <= 1099511627775, 5,
        v <= 281474976710655, 6,
        v <= 72057594037927935, 7,
        8
    ) AS be_len_u64,

    -- Total size of an RLP-encoded unsigned integer
    (v) -> if(v = 0, 1, if(v < 128, 1, 1 + be_len_u64(v))) AS rlp_u64_size,

    -- Aggregate log RLP sizes per transaction
    logs_per_tx AS (
        SELECT
            block_number,
            transaction_hash,
            toUInt32(count()) AS log_count,
            sum(hex_bytes(ifNull(data, ''))) AS log_data_bytes,
            toUInt32(sum(
                if(topic0 != '' AND topic0 != '0x', 1, 0)
                + if(isNotNull(topic1) AND topic1 != '' AND topic1 != '0x', 1, 0)
                + if(isNotNull(topic2) AND topic2 != '' AND topic2 != '0x', 1, 0)
                + if(isNotNull(topic3) AND topic3 != '' AND topic3 != '0x', 1, 0)
            )) AS log_topic_count,
            sum(log_rlp_size) AS logs_payload_bytes
        FROM (
            SELECT
                block_number,
                transaction_hash,
                topic0, topic1, topic2, topic3, data,
                (
                    if(topic0 != '' AND topic0 != '0x', 1, 0)
                    + if(isNotNull(topic1) AND topic1 != '' AND topic1 != '0x', 1, 0)
                    + if(isNotNull(topic2) AND topic2 != '' AND topic2 != '0x', 1, 0)
                    + if(isNotNull(topic3) AND topic3 != '' AND topic3 != '0x', 1, 0)
                ) AS n_topics,
                hex_bytes(ifNull(data, '')) AS data_bytes,
                -- First byte of data for RLP single-byte optimization
                if(
                    hex_bytes(ifNull(data, '')) = 1,
                    reinterpretAsUInt8(unhex(
                        if(startsWith(ifNull(data, ''), '0x'),
                            substring(ifNull(data, ''), 3, 2),
                            substring(ifNull(data, ''), 1, 2))
                    )),
                    0
                ) AS data_first_byte,
                -- RLP size of this log: RLP([address(20), RLP([topics...]), RLP(data)])
                rlp_list_size(
                    21                                          -- address: 20 bytes + 1 byte RLP header
                    + rlp_list_size(33 * n_topics)              -- topics list: 32+1 bytes each
                    + rlp_bytes_size(data_bytes, data_first_byte) -- data bytes
                ) AS log_rlp_size
            FROM {{ index .dep "{{external}}" "canonical_execution_logs" "helpers" "from" }}
            WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
                AND meta_network_name = '{{ .env.NETWORK }}'
        )
        GROUP BY block_number, transaction_hash
    ),

    -- Transaction data with cumulative gas (needed for receipt RLP size)
    tx AS (
        SELECT
            block_number,
            meta_network_name,
            transaction_index,
            transaction_hash,
            -- Legacy transactions may arrive with nullable type in some sources.
            -- Normalize NULL to 0 so we do not add a typed receipt prefix byte.
            ifNull(transaction_type, toUInt32(0)) AS transaction_type,
            toUInt64(success) AS status_u64,
            gas_used,
            sum(gas_used) OVER (
                PARTITION BY block_number, meta_network_name
                ORDER BY transaction_index
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_gas_used
        FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            AND meta_network_name = '{{ .env.NETWORK }}'
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    tx.block_number,
    tx.transaction_hash,
    tx.transaction_index,
    -- Receipt RLP size:
    -- Type 0 (legacy): no type prefix
    -- Type > 0 (typed): 1 byte type prefix
    toUInt64(
        if(tx.transaction_type = 0, 0, 1)
        + rlp_list_size(
            rlp_u64_size(tx.status_u64)
            + rlp_u64_size(tx.cumulative_gas_used)
            + 259  -- logs bloom: 256 bytes + 3 bytes RLP string header
            + rlp_list_size(ifNull(l.logs_payload_bytes, 0))
        )
    ) AS receipt_bytes,
    COALESCE(l.log_count, toUInt32(0)) AS log_count,
    COALESCE(l.log_data_bytes, toUInt64(0)) AS log_data_bytes,
    COALESCE(l.log_topic_count, toUInt32(0)) AS log_topic_count,
    tx.meta_network_name
FROM tx
LEFT JOIN logs_per_tx l
    ON l.block_number = tx.block_number
    AND l.transaction_hash = tx.transaction_hash
