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
        block_root,
        max(epoch) OVER () AS max_epoch
    FROM `{{ index .dep "{{external}}" "canonical_beacon_block" "database" }}`.`canonical_beacon_block` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

all_blocks AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block AS block_root,
        NULL AS proposer_index
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_events_block_gossip" "database" }}`.`beacon_api_eth_v1_events_block_gossip` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    
    UNION DISTINCT
    
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block AS block_root,
        NULL AS proposer_index
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_events_block" "database" }}`.`beacon_api_eth_v1_events_block` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    
    UNION DISTINCT
    
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

orphaned_blocks AS (
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        argMax(proposer_index, proposer_index IS NOT NULL) AS proposer_index
    FROM all_blocks
    WHERE epoch <= (SELECT any(max_epoch) FROM canonical)
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    ob.slot AS slot,
    ob.slot_start_date_time AS slot_start_date_time,
    ob.epoch AS epoch,
    ob.epoch_start_date_time AS epoch_start_date_time,
    ob.block_root AS block_root,
    ob.proposer_index AS proposer_index
FROM orphaned_blocks ob
LEFT ANTI JOIN canonical c
    ON ob.slot = c.slot AND ob.block_root = c.block_root;
