---
table: fct_block_payload
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
  - epbs
  - canonical
dependencies:
  - "{{external}}.canonical_beacon_block_execution_payload_bid"
  - "{{external}}.canonical_beacon_block_execution_transaction"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas (ePBS): per-block execution payload facts. The beacon block no longer
-- carries the payload, so the pre-gloas fct_block execution columns are empty
-- in this era. The winning bid provides the committed hash, gas limit and
-- blob count, and the envelope-derived transaction table provides what the
-- payload actually contained. Bid amounts are Gwei on the wire and stored as
-- wei to match the mev_relay bid tables.
WITH bids AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        block_version,
        builder_index,
        block_hash,
        parent_block_hash,
        toUInt128(`value`) * 1000000000 AS `value`,
        gas_limit,
        blob_kzg_commitment_count
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_payload_bid" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),
transaction_totals AS (
    SELECT
        block_root,
        count() AS transactions_count,
        SUM(size) AS transactions_total_bytes,
        SUM(gas) AS transactions_total_gas_limit,
        countIf(type = 3) AS blob_transactions_count
    FROM {{ index .dep "{{external}}" "canonical_beacon_block_execution_transaction" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY block_root
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    b.slot AS slot,
    b.slot_start_date_time AS slot_start_date_time,
    b.epoch AS epoch,
    b.epoch_start_date_time AS epoch_start_date_time,
    b.block_root AS block_root,
    b.block_version AS block_version,
    b.builder_index AS builder_index,
    b.block_hash AS block_hash,
    b.parent_block_hash AS parent_block_hash,
    b.`value` AS `value`,
    b.gas_limit AS gas_limit,
    b.blob_kzg_commitment_count AS blob_kzg_commitment_count,
    COALESCE(t.transactions_count, 0) AS transactions_count,
    COALESCE(t.transactions_total_bytes, 0) AS transactions_total_bytes,
    COALESCE(t.transactions_total_gas_limit, 0) AS transactions_total_gas_limit,
    COALESCE(t.blob_transactions_count, 0) AS blob_transactions_count
FROM bids b
GLOBAL LEFT JOIN transaction_totals t ON b.block_root = t.block_root
SETTINGS join_use_nulls = 1
