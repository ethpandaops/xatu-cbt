---
table: fct_block_proposer_head
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
  - "{{external}}.beacon_api_eth_v1_events_block_gossip"
  - "{{external}}.beacon_api_eth_v1_events_block"
  - "{{external}}.beacon_api_eth_v1_proposer_duty"
  - "{{external}}.libp2p_gossipsub_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH proposer_duties AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        proposer_validator_index,
        proposer_pubkey
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_proposer_duty" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

block_gossip AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block as block_root
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_block_gossip" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

block_events AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block as block_root
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

gossipsub_blocks AS (
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block as block_root,
        proposer_index
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

all_blocks AS (
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        NULL as proposer_index
    FROM block_gossip
    
    UNION ALL
    
    SELECT 
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        NULL as proposer_index
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
        argMax(proposer_index, proposer_index IS NOT NULL) as proposer_index
    FROM all_blocks
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    COALESCE(pd.slot, db.slot) as slot,
    COALESCE(pd.slot_start_date_time, db.slot_start_date_time) as slot_start_date_time,
    COALESCE(pd.epoch, db.epoch) as epoch,
    COALESCE(pd.epoch_start_date_time, db.epoch_start_date_time) as epoch_start_date_time,
    COALESCE(pd.proposer_validator_index, db.proposer_index) as proposer_validator_index,
    pd.proposer_pubkey as proposer_pubkey,
    NULLIF(db.block_root, '') AS block_root
FROM proposer_duties pd
FULL OUTER JOIN deduplicated_blocks db ON pd.slot = db.slot
SETTINGS join_use_nulls = 1;
