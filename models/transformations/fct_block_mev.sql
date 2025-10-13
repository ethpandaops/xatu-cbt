---
table: fct_block_mev
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
  - canonical
dependencies:
  - "{{transformation}}.int_block_mev_canonical"
  - "{{transformation}}.fct_block_mev_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH canonical_blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        earliest_bid_date_time,
        relay_names,
        parent_hash,
        block_number,
        block_hash,
        builder_pubkey,
        proposer_pubkey,
        proposer_fee_recipient,
        gas_limit,
        gas_used,
        value,
        transaction_count,
        'canonical' AS `status`
    FROM `{{ index .dep "{{transformation}}" "int_block_mev_canonical" "database" }}`.`int_block_mev_canonical` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
orphaned_blocks AS (
    SELECT
        h.slot AS slot,
        h.slot_start_date_time AS slot_start_date_time,
        h.epoch AS epoch,
        h.epoch_start_date_time AS epoch_start_date_time,
        h.block_root AS block_root,
        h.earliest_bid_date_time AS earliest_bid_date_time,
        h.relay_names AS relay_names,
        h.parent_hash AS parent_hash,
        h.block_number AS block_number,
        h.block_hash AS block_hash,
        h.builder_pubkey AS builder_pubkey,
        h.proposer_pubkey AS proposer_pubkey,
        h.proposer_fee_recipient AS proposer_fee_recipient,
        h.gas_limit AS gas_limit,
        h.gas_used AS gas_used,
        h.value AS value,
        h.transaction_count AS transaction_count,
        'orphaned' AS `status`
    FROM `{{ index .dep "{{transformation}}" "fct_block_mev_head" "database" }}`.`fct_block_mev_head` AS h FINAL
    GLOBAL LEFT ANTI JOIN canonical_blocks c 
        ON h.slot_start_date_time = c.slot_start_date_time 
        AND h.block_root = c.block_root
    WHERE h.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    *
FROM (
    SELECT * FROM canonical_blocks
    UNION ALL
    SELECT * FROM orphaned_blocks
)
