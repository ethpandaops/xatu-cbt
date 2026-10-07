---
table: fct_block_mev_head
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - mev
  - head
dependencies:
  - "{{transformation}}.fct_block_head"
  # Each group is the relay source and its Gloas (ePBS) replacement: the
  # delivered payload or the revealed payload, the earliest bid trace or
  # gossip bid, and the builder pubkey from the relay or the builder registry.
  - [
    "{{external}}.mev_relay_proposer_payload_delivered",
    "{{external}}.beacon_api_eth_v1_events_execution_payload"
  ]
  - [
    "{{external}}.mev_relay_bid_trace",
    "{{external}}.beacon_api_eth_v1_events_execution_payload_bid"
  ]
  - [
    "{{external}}.mev_relay_proposer_payload_delivered",
    "{{external}}.canonical_beacon_state_builder"
  ]
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Relay-delivered blocks, plus Gloas blocks whose revealed payload came from
-- an external builder. Self-built Gloas blocks are excluded, as locally built
-- blocks are before Gloas. The winning bid's terms come from gossip, so a bid
-- sent straight to the proposer leaves value and fee recipient unknown. A
-- Gloas block's value is the bid value plus its execution payment, Gwei on
-- the wire.
WITH blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        execution_payload_block_hash AS block_hash,
        execution_payload_parent_hash AS parent_hash,
        execution_payload_block_number AS block_number,
        execution_payload_gas_used AS execution_gas_used,
        execution_payload_transactions_count AS execution_transaction_count
    FROM {{ index .dep "{{transformation}}" "fct_block_head" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
proposer_payloads_raw AS (
  SELECT
    block_hash,
    builder_pubkey,
    proposer_pubkey,
    proposer_fee_recipient,
    gas_limit,
    gas_used,
    value,
    num_tx AS transaction_count,
    relay_name
  FROM {{ index .dep "{{external}}" "mev_relay_proposer_payload_delivered" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
),
bid_traces_raw AS (
  SELECT
    block_hash,
    min(timestamp_ms) AS min_timestamp_ms
  FROM {{ index .dep "{{external}}" "mev_relay_bid_trace" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
  GROUP BY block_hash
),
blocks_with_payloads AS (
  SELECT
    b.*,
    p.builder_pubkey,
    p.proposer_pubkey,
    p.proposer_fee_recipient,
    p.gas_limit,
    p.gas_used,
    p.value,
    p.transaction_count,
    p.relay_name
  FROM blocks b
  GLOBAL INNER JOIN proposer_payloads_raw p ON b.block_hash = p.block_hash
),
payload_aggregated AS (
  SELECT
    block_hash,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    parent_hash,
    block_number,
    any(builder_pubkey) AS builder_pubkey,
    any(proposer_pubkey) AS proposer_pubkey,
    any(proposer_fee_recipient) AS proposer_fee_recipient,
    any(gas_limit) AS gas_limit,
    any(gas_used) AS gas_used,
    any(transaction_count) AS transaction_count,
    groupArray(DISTINCT relay_name) AS relay_names,
    max(value) AS value
  FROM blocks_with_payloads
  GROUP BY block_hash, slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, parent_hash, block_number
),
relay_blocks AS (
  SELECT
    pa.slot,
    pa.slot_start_date_time,
    pa.epoch,
    pa.epoch_start_date_time,
    pa.block_root,
    CASE
      WHEN bt.min_timestamp_ms != 0
      THEN toDateTime64(bt.min_timestamp_ms / 1000, 3)
      ELSE NULL
    END AS earliest_bid_date_time,
    pa.relay_names,
    pa.parent_hash,
    pa.block_number,
    pa.block_hash,
    pa.builder_pubkey,
    pa.proposer_pubkey,
    pa.proposer_fee_recipient,
    pa.gas_limit,
    pa.gas_used,
    CASE
      WHEN pa.value != 0
      THEN pa.value
      ELSE NULL
    END AS value,
    pa.transaction_count
  FROM payload_aggregated pa
  GLOBAL LEFT JOIN bid_traces_raw bt ON pa.block_hash = bt.block_hash
),
revealed_payloads AS (
  SELECT
    block_root,
    any(builder_index) AS payload_builder_index,
    any(block_hash) AS payload_block_hash
  FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND builder_index IS NOT NULL
    AND builder_index != 18446744073709551615
  GROUP BY block_root
),
gossip_bids AS (
  SELECT
    block_hash,
    toNullable(min(event_date_time)) AS earliest_bid_date_time,
    any(parent_block_hash) AS bid_parent_block_hash,
    any(fee_recipient) AS bid_fee_recipient,
    any(gas_limit) AS bid_gas_limit,
    toNullable((toUInt128(any(`value`)) + toUInt128(any(execution_payment))) * 1000000000) AS bid_value
  FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload_bid" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
  GROUP BY block_hash
),
builders AS (
  SELECT
    epoch,
    builder_index,
    toString(any(pubkey)) AS builder_pubkey
  FROM {{ index .dep "{{external}}" "canonical_beacon_state_builder" "helpers" "from" }} FINAL
  -- The epoch holding the first slot starts up to one epoch before it.
  WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 384 SECOND
      AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
  GROUP BY epoch, builder_index
),
builder_blocks AS (
  SELECT
    b.slot AS slot,
    b.slot_start_date_time AS slot_start_date_time,
    b.epoch AS epoch,
    b.epoch_start_date_time AS epoch_start_date_time,
    b.block_root AS block_root,
    gb.earliest_bid_date_time AS earliest_bid_date_time,
    CAST([], 'Array(String)') AS relay_names,
    gb.bid_parent_block_hash AS parent_hash,
    b.block_number AS block_number,
    rp.payload_block_hash AS block_hash,
    bu.builder_pubkey AS builder_pubkey,
    '' AS proposer_pubkey,
    gb.bid_fee_recipient AS proposer_fee_recipient,
    gb.bid_gas_limit AS gas_limit,
    ifNull(b.execution_gas_used, 0) AS gas_used,
    if(gb.bid_value != 0, gb.bid_value, NULL) AS `value`,
    ifNull(b.execution_transaction_count, 0) AS transaction_count
  FROM blocks b
  GLOBAL INNER JOIN revealed_payloads rp ON b.block_root = rp.block_root
  GLOBAL LEFT JOIN gossip_bids gb ON rp.payload_block_hash = gb.block_hash
  GLOBAL LEFT JOIN builders bu ON b.epoch = bu.epoch AND rp.payload_builder_index = bu.builder_index
  WHERE b.block_root GLOBAL NOT IN (SELECT block_root FROM relay_blocks)
)

SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  *
FROM (
  SELECT * FROM relay_blocks
  UNION ALL
  SELECT * FROM builder_blocks
)
