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
  - "{{external}}.beacon_api_eth_v1_beacon_blob"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    any(slot) AS slot,
    slot_start_date_time,
    any(epoch) AS epoch,
    any(epoch_start_date_time) AS epoch_start_date_time,
    block_root,
    blob_index,
    any(versioned_hash) AS versioned_hash,
    any(kzg_commitment) AS kzg_commitment
FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_blob" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, block_root, blob_index
