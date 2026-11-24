---
table: int_data_column_gossip_by_slot
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - data_column
  - peerdas
  - custody
dependencies:
  - "{{external}}.libp2p_gossipsub_data_column_sidecar"
  - "{{external}}.beacon_api_eth_v1_events_data_column_sidecar"
---
-- Gossipsub sightings of each data column per slot. Sightings arrive within seconds of the slot, so each slot is
-- counted once. fct_data_column_availability_by_slot joins these onto the custody probe counts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
-- libp2p sentries record gossip before validating it, and some relays (gnode-beacon) re-encode
-- sidecars, which changes the block root and breaks the header signature. A gossiped sidecar counts
-- only if a beacon node accepted a sidecar for its root. Beacon nodes emit the event after validation.
WITH validated_roots AS (
    SELECT DISTINCT if(startsWith(toString(block_root), '0x'), toString(block_root), concat('0x', toStringCutToZero(block_root))) AS block_root
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_data_column_sidecar" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    column_index,
    -- The earliest sighting's block root, and how many roots were seen (more than 1 means a fork)
    argMin(root, propagation_slot_start_diff) AS beacon_block_root,
    toUInt8(uniqExact(root)) AS beacon_block_root_variants,
    toUInt16(max(kzg_commitments_count)) AS blob_count,
    toUInt32(count()) AS gossipsub_count
FROM (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        column_index,
        if(startsWith(beacon_block_root, '0x'), toString(beacon_block_root), concat('0x', toStringCutToZero(beacon_block_root))) AS root,
        propagation_slot_start_diff,
        kzg_commitments_count
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_data_column_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
)
WHERE root GLOBAL IN (SELECT block_root FROM validated_roots)
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, column_index
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
