---
table: fct_block_payload_bid
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - payload
  - bid
  - epbs
  - canonical
dependencies:
  - "{{external}}.canonical_beacon_block_execution_payload_bid"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas (ePBS): the winning execution payload bid committed in each canonical
-- beacon block (one per block). A bid whose builder index equals the block
-- proposer's validator index is a self-build. Bid amounts are Gwei on the
-- wire; stored as wei to match the mev_relay bid tables.
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    block_version,
    builder_index,
    block_hash,
    parent_block_hash,
    parent_block_root,
    toUInt128(`value`) * 1000000000 AS `value`,
    toUInt128(execution_payment) * 1000000000 AS execution_payment,
    fee_recipient,
    gas_limit,
    blob_kzg_commitment_count
FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_payload_bid" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
