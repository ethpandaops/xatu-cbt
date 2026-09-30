---
table: fct_block_head
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - proposer
  - head
dependencies:
  - "{{external}}.beacon_api_eth_v2_beacon_block"
  # Gloas payload sources (revealed payload hash, EL execution results).
  # OR-grouped with the block SSE events (CBT rejects duplicate edges, so the
  # v2 block table cannot appear twice) so networks where these tables are
  # empty or absent schedule unaffected. The plain v2_beacon_block
  # dependency above still caps the processable range - bounds take the
  # minimum across deps.
  - - "{{external}}.beacon_api_eth_v1_events_execution_payload"
    - "{{external}}.execution_engine_new_payload"
    - "{{external}}.beacon_api_eth_v1_events_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Gloas (ePBS): the beacon block no longer embeds the execution payload, so
-- its execution_payload_block_hash is empty in the block table. The
-- execution_payload SSE events map each revealed payload's block hash to its
-- beacon block root. Empty on pre-gloas networks, where it contributes nothing.
payload_events AS (
    SELECT
        block_root,
        any(block_hash) AS payload_block_hash
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload" "helpers" "from" }}
    WHERE meta_network_name = '{{ .env.NETWORK }}'
        AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND block_hash != ''
    GROUP BY block_root
),
-- What each revealed payload executed to, from engine_newPayload
-- observations of its content-addressed hash (exact whenever observed). The
-- winning bid is not on the head stream for every block (self-builds and bids
-- delivered off-p2p are never gossiped), so the EL is the only complete source
-- here and the fee recipient stays NULL on gloas head rows.
engine_payloads AS (
    SELECT
        block_hash,
        any(block_number) AS block_number,
        any(parent_hash) AS parent_hash,
        any(gas_limit) AS gas_limit,
        any(gas_used) AS gas_used,
        any(tx_count) AS tx_count,
        any(blob_count) AS blob_count
    FROM {{ index .dep "{{external}}" "execution_engine_new_payload" "helpers" "from" }}
    WHERE meta_network_name = '{{ .env.NETWORK }}'
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
        AND (SELECT count() FROM payload_events) > 0
        AND status = 'VALID'
        AND block_hash GLOBAL IN (SELECT payload_block_hash FROM payload_events)
    GROUP BY block_hash
),
payloads AS (
    SELECT
        pe.block_root AS block_root,
        pe.payload_block_hash AS payload_block_hash,
        ep.parent_hash AS parent_hash,
        ep.gas_limit AS gas_limit,
        -- GAS_PER_BLOB = 2**17
        toUInt64(ep.blob_count) * 131072 AS blob_gas_used,
        ep.block_number AS block_number,
        ep.gas_used AS gas_used,
        ep.tx_count AS tx_count
    FROM payload_events pe
    GLOBAL LEFT JOIN engine_payloads ep ON pe.payload_block_hash = ep.block_hash
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMax(slot, updated_date_time) AS slot,
    slot_start_date_time,
    argMax(epoch, updated_date_time) AS epoch,
    argMax(epoch_start_date_time, updated_date_time) AS epoch_start_date_time,
    bb.block_root AS block_root,
    argMax(block_version, updated_date_time) AS block_version,
    argMax(block_total_bytes, updated_date_time) AS block_total_bytes,
    argMax(block_total_bytes_compressed, updated_date_time) AS block_total_bytes_compressed,
    argMax(parent_root, updated_date_time) AS parent_root,
    argMax(state_root, updated_date_time) AS state_root,
    argMax(proposer_index, updated_date_time) AS proposer_index,
    argMax(eth1_data_block_hash, updated_date_time) AS eth1_data_block_hash,
    argMax(eth1_data_deposit_root, updated_date_time) AS eth1_data_deposit_root,
    -- Pre-gloas blocks carry their payload inline. Gloas blocks fall back to
    -- the payload revealed for their block root on the SSE layer and its
    -- engine_newPayload execution results
    coalesce(nullif(argMax(execution_payload_block_hash, updated_date_time), ''), any(pe.payload_block_hash), '') AS execution_payload_block_hash,
    coalesce(argMax(execution_payload_block_number, updated_date_time), any(pe.block_number)) AS execution_payload_block_number,
    argMax(execution_payload_fee_recipient, updated_date_time) AS execution_payload_fee_recipient,
    argMax(execution_payload_base_fee_per_gas, updated_date_time) AS execution_payload_base_fee_per_gas,
    coalesce(argMax(execution_payload_blob_gas_used, updated_date_time), any(pe.blob_gas_used)) AS execution_payload_blob_gas_used,
    argMax(execution_payload_excess_blob_gas, updated_date_time) AS execution_payload_excess_blob_gas,
    coalesce(argMax(execution_payload_gas_limit, updated_date_time), any(pe.gas_limit)) AS execution_payload_gas_limit,
    coalesce(argMax(execution_payload_gas_used, updated_date_time), any(pe.gas_used)) AS execution_payload_gas_used,
    argMax(execution_payload_state_root, updated_date_time) AS execution_payload_state_root,
    coalesce(nullif(argMax(execution_payload_parent_hash, updated_date_time), ''), any(pe.parent_hash), '') AS execution_payload_parent_hash,
    -- Gloas blocks report zero transactions (they live in the envelope), so
    -- switch source on a revealed payload rather than coalescing
    if(any(pe.block_root) IS NOT NULL, any(pe.tx_count), argMax(execution_payload_transactions_count, updated_date_time)) AS execution_payload_transactions_count,
    if(any(pe.block_root) IS NOT NULL, NULL, argMax(execution_payload_transactions_total_bytes, updated_date_time)) AS execution_payload_transactions_total_bytes,
    if(any(pe.block_root) IS NOT NULL, NULL, argMax(execution_payload_transactions_total_bytes_compressed, updated_date_time)) AS execution_payload_transactions_total_bytes_compressed
FROM {{ index .dep "{{external}}" "beacon_api_eth_v2_beacon_block" "helpers" "from" }} AS bb FINAL
GLOBAL LEFT JOIN payloads pe ON bb.block_root = pe.block_root
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, block_root
SETTINGS join_use_nulls = 1
