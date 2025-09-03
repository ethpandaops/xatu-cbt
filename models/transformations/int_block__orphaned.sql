---
table: int_block__orphaned
interval:
  max: 500000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - orphaned
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_block_gossip"
  - "{{external}}.beacon_api_eth_v1_events_block"
  - "{{external}}.libp2p_gossipsub_beacon_block"
  - "{{external}}.canonical_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH canonical AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root
    FROM `{{ index .dep "{{external}}" "canonical_beacon_block" "database" }}`.`canonical_beacon_block` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

max_canonical_epoch AS (
    SELECT max(epoch) AS max_epoch FROM canonical
),

block_gossip AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block AS block_root
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_events_block_gossip" "database" }}`.`beacon_api_eth_v1_events_block_gossip` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

block_events AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block AS block_root
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_events_block" "database" }}`.`beacon_api_eth_v1_events_block` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

gossipsub_blocks AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block AS block_root,
        proposer_index
    FROM `{{ index .dep "{{external}}" "libp2p_gossipsub_beacon_block" "database" }}`.`libp2p_gossipsub_beacon_block` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

all_blocks AS (
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        NULL AS proposer_index
    FROM block_gossip

    UNION ALL

    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        NULL AS proposer_index
    FROM block_events

    UNION ALL

    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        proposer_index
    FROM gossipsub_blocks
),

deduplicated_blocks AS (
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        argMax(proposer_index, proposer_index IS NOT NULL) AS proposer_index
    FROM all_blocks
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
),

filtered_candidates AS (
    SELECT *
    FROM deduplicated_blocks
    WHERE epoch <= (SELECT max_epoch FROM max_canonical_epoch)
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    db.slot AS slot,
    db.slot_start_date_time AS slot_start_date_time,
    db.epoch AS epoch,
    db.epoch_start_date_time AS epoch_start_date_time,
    CASE WHEN db.block_root = '' THEN NULL ELSE db.block_root END AS block_root,
    db.proposer_index AS proposer_index
FROM filtered_candidates db
LEFT ANTI JOIN canonical c
    ON db.slot = c.slot AND db.block_root = c.block_root;

