---
table: int_block_mev_canonical
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - mev
  - canonical
dependencies:
  - "{{transformation}}.int_block_canonical"
  # Each group is the relay source and its Gloas (ePBS) replacement: the
  # delivered payload or the block's winning bid, the earliest bid trace or
  # gossip bid, and the builder pubkey from the relay or the builder registry.
  - [
    "{{external}}.mev_relay_proposer_payload_delivered",
    "{{external}}.canonical_beacon_block_execution_payload_bid"
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
-- Relay-delivered blocks, plus Gloas blocks won by an external builder's bid.
-- Self-built Gloas blocks have no builder index and are excluded, as locally
-- built blocks are before Gloas. A Gloas block's value is what the proposer
-- is paid: the bid value plus its execution payment, Gwei on the wire.
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
    FROM {{ index .dep "{{transformation}}" "int_block_canonical" "helpers" "from" }} FINAL
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
winning_bids AS (
  SELECT
    block_root,
    builder_index,
    block_hash,
    parent_block_hash,
    fee_recipient,
    gas_limit,
    (toUInt128(`value`) + toUInt128(execution_payment)) * 1000000000 AS `value`
  FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_payload_bid" "helpers" "from" }} FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND builder_index IS NOT NULL
    AND builder_index != 18446744073709551615
),
gossip_bids_first_seen AS (
  SELECT
    block_hash,
    toNullable(min(event_date_time)) AS earliest_bid_date_time
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
    fs.earliest_bid_date_time AS earliest_bid_date_time,
    CAST([], 'Array(String)') AS relay_names,
    wb.parent_block_hash AS parent_hash,
    b.block_number AS block_number,
    wb.block_hash AS block_hash,
    bu.builder_pubkey AS builder_pubkey,
    '' AS proposer_pubkey,
    wb.fee_recipient AS proposer_fee_recipient,
    wb.gas_limit AS gas_limit,
    ifNull(b.execution_gas_used, 0) AS gas_used,
    if(wb.`value` != 0, toNullable(wb.`value`), NULL) AS `value`,
    ifNull(b.execution_transaction_count, 0) AS transaction_count
  FROM blocks b
  GLOBAL INNER JOIN winning_bids wb ON b.block_root = wb.block_root
  GLOBAL LEFT JOIN gossip_bids_first_seen fs ON wb.block_hash = fs.block_hash
  GLOBAL LEFT JOIN builders bu ON b.epoch = bu.epoch AND wb.builder_index = bu.builder_index
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
