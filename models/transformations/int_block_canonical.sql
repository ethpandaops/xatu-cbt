---
table: int_block_canonical
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - proposer
  - canonical
dependencies:
  - "{{external}}.canonical_beacon_block"
  # Gloas (ePBS) payload sources, only read for blocks that carry a payload
  # bid. OR-grouped with the always-populated head block table so pre-gloas
  # networks (empty bid table) and networks without the EL engine snooper
  # schedule unaffected. The plain canonical_beacon_block dependency above
  # still caps the processable range - bounds take the minimum across deps -
  # and the canonical bid is derived by cannon in lockstep with the block.
  - - "{{external}}.canonical_beacon_block_execution_payload_bid"
    - "{{external}}.execution_engine_new_payload"
    - "{{external}}.beacon_api_eth_v2_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Gloas (ePBS): the beacon block no longer embeds the execution payload, so
-- the execution_payload_* columns of canonical_beacon_block are empty. The
-- block commits to its payload through the winning bid, which carries the
-- committed block hash, parent hash, fee recipient, gas limit and blob count.
-- Empty on pre-gloas networks, where every column below falls through to the
-- block's own values unchanged.
payload_bids AS (
    SELECT
        block_root,
        block_hash,
        parent_block_hash,
        fee_recipient,
        gas_limit,
        blob_kzg_commitment_count
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_payload_bid" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- What the revealed payload actually executed to (block number, gas used,
-- transaction count) is only known to the EL. engine_newPayload observations
-- are keyed by the content-addressed block hash, so any VALID observation of
-- the committed hash is exact. Withheld payloads are never sent to the EL and
-- stay NULL. The scan window is derived from the bids, so it is empty (and
-- prunes to nothing) on pre-gloas networks.
engine_payloads AS (
    SELECT
        block_hash,
        any(block_number) AS block_number,
        any(gas_used) AS gas_used,
        any(tx_count) AS tx_count
    FROM {{ index .dep "{{external}}" "execution_engine_new_payload" "helpers" "from" }}
    WHERE meta_network_name = '{{ .env.NETWORK }}'
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
        AND (SELECT count() FROM payload_bids) > 0
        AND status = 'VALID'
        AND block_hash GLOBAL IN (SELECT block_hash FROM payload_bids)
    GROUP BY block_hash
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    b.slot AS slot,
    b.slot_start_date_time AS slot_start_date_time,
    b.epoch AS epoch,
    b.epoch_start_date_time AS epoch_start_date_time,
    b.block_root AS block_root,
    b.block_version AS block_version,
    b.block_total_bytes AS block_total_bytes,
    b.block_total_bytes_compressed AS block_total_bytes_compressed,
    b.parent_root AS parent_root,
    b.state_root AS state_root,
    b.proposer_index AS proposer_index,
    b.eth1_data_block_hash AS eth1_data_block_hash,
    b.eth1_data_deposit_root AS eth1_data_deposit_root,
    -- Pre-gloas blocks carry their payload inline. Gloas blocks take the hash
    -- they committed to in the bid (also for payloads later withheld or not
    -- built upon, see fct_block_payload.payload_status).
    coalesce(b.execution_payload_block_hash, pb.block_hash) AS execution_payload_block_hash,
    coalesce(b.execution_payload_block_number, ep.block_number) AS execution_payload_block_number,
    coalesce(b.execution_payload_fee_recipient, pb.fee_recipient) AS execution_payload_fee_recipient,
    -- Not committed in the bid nor reported by engine_newPayload: NULL on gloas.
    b.execution_payload_base_fee_per_gas AS execution_payload_base_fee_per_gas,
    -- GAS_PER_BLOB = 2**17. The envelope must carry exactly the committed blobs.
    coalesce(b.execution_payload_blob_gas_used, toUInt64(pb.blob_kzg_commitment_count) * 131072) AS execution_payload_blob_gas_used,
    b.execution_payload_excess_blob_gas AS execution_payload_excess_blob_gas,
    coalesce(b.execution_payload_gas_limit, pb.gas_limit) AS execution_payload_gas_limit,
    coalesce(b.execution_payload_gas_used, ep.gas_used) AS execution_payload_gas_used,
    b.execution_payload_state_root AS execution_payload_state_root,
    coalesce(b.execution_payload_parent_hash, pb.parent_block_hash) AS execution_payload_parent_hash,
    -- The gloas block reports zero transactions (they live in the envelope), so
    -- switch source on the presence of a bid rather than coalescing.
    if(pb.block_root IS NOT NULL, ep.tx_count, b.execution_payload_transactions_count) AS execution_payload_transactions_count,
    if(pb.block_root IS NOT NULL, NULL, b.execution_payload_transactions_total_bytes) AS execution_payload_transactions_total_bytes,
    if(pb.block_root IS NOT NULL, NULL, b.execution_payload_transactions_total_bytes_compressed) AS execution_payload_transactions_total_bytes_compressed
FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} AS b FINAL
GLOBAL LEFT JOIN payload_bids pb ON b.block_root = pb.block_root
GLOBAL LEFT JOIN engine_payloads ep ON pb.block_hash = ep.block_hash
WHERE b.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND b.meta_network_name = '{{ .env.NETWORK }}'
SETTINGS join_use_nulls = 1
