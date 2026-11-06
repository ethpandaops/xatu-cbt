---
table: fct_slot_summary
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
  - canonical
dependencies:
  - "{{transformation}}.fct_block_proposer"
  - "{{transformation}}.fct_block"
  - "{{transformation}}.fct_block_blob_count"
  - "{{transformation}}.fct_block_graffiti"
  - "{{transformation}}.dim_node"
---
-- One row per slot: whether it was proposed, missed or orphaned, who proposed it, and the block's size and clients,
-- so a slot list can filter and page on the server instead of joining per-slot tables in the browser.
-- MEV relays, builder and value stay in int_block_mev_canonical, which starts at the merge.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
    proposers AS (
        SELECT slot, slot_start_date_time, epoch, epoch_start_date_time, proposer_validator_index, proposer_pubkey, block_root, status
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    blocks AS (
        SELECT
            slot_start_date_time,
            toString(block_root) AS block_root,
            execution_payload_block_number,
            execution_payload_gas_used,
            execution_payload_gas_limit,
            execution_payload_transactions_count
        FROM {{ index .dep "{{transformation}}" "fct_block" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    blobs AS (
        SELECT slot_start_date_time, block_root, blob_count
        FROM {{ index .dep "{{transformation}}" "fct_block_blob_count" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    graffiti AS (
        SELECT slot_start_date_time, block_root, consensus_client, execution_client
        FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    nodes AS (
        SELECT validator_index, source
        FROM {{ index .dep "{{transformation}}" "dim_node" "helpers" "from" }} FINAL
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    p.slot AS slot,
    p.slot_start_date_time AS slot_start_date_time,
    p.epoch AS epoch,
    p.epoch_start_date_time AS epoch_start_date_time,
    p.status AS status,
    p.block_root AS block_root,
    p.proposer_validator_index AS proposer_validator_index,
    -- Entity is the current dim_node mapping of the proposer, not a historical snapshot
    n.source AS entity,
    -- Missed slots have no block
    if(p.block_root IS NULL, NULL, nullIf(b.execution_payload_block_number, 0)) AS block_number,
    if(p.block_root IS NULL, NULL, b.execution_payload_gas_used) AS gas_used,
    if(p.block_root IS NULL, NULL, b.execution_payload_gas_limit) AS gas_limit,
    if(p.block_root IS NULL, NULL, b.execution_payload_transactions_count) AS transaction_count,
    -- A block without a blob count row carries no blobs
    if(p.block_root IS NULL, NULL, toUInt32(ifNull(bl.blob_count, 0))) AS blob_count,
    if(p.block_root IS NULL, NULL, g.consensus_client) AS consensus_client,
    if(p.block_root IS NULL, NULL, g.execution_client) AS execution_client
FROM proposers AS p
GLOBAL LEFT JOIN blocks AS b ON p.slot_start_date_time = b.slot_start_date_time AND p.block_root = b.block_root
GLOBAL LEFT JOIN blobs AS bl ON p.slot_start_date_time = bl.slot_start_date_time AND p.block_root = bl.block_root
GLOBAL LEFT JOIN graffiti AS g ON p.slot_start_date_time = g.slot_start_date_time AND p.block_root = g.block_root
GLOBAL LEFT JOIN nodes AS n ON p.proposer_validator_index = n.validator_index AND NOT (p.proposer_validator_index = 0 AND p.proposer_pubkey = '')
SETTINGS join_use_nulls = 1
