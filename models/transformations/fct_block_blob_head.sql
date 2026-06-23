---
table: fct_block_blob_head
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
  - blob
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_blob_sidecar"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, propagation_slot_start_diff) AS slot,
    slot_start_date_time,
    argMin(epoch, propagation_slot_start_diff) AS epoch,
    argMin(epoch_start_date_time, propagation_slot_start_diff) AS epoch_start_date_time,
    block_root,
    blob_index,
    argMin(versioned_hash, propagation_slot_start_diff) AS versioned_hash,
    argMin(kzg_commitment, propagation_slot_start_diff) AS kzg_commitment
FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_blob_sidecar" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, block_root, blob_index
