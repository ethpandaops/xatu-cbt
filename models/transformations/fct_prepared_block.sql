---
table: fct_prepared_block
interval:
  max: 384
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - prepared
  - block
  - locally-built
dependencies:
  - "{{external}}.beacon_api_eth_v3_validator_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    event_date_time,
    meta_client_name,
    meta_client_version,
    meta_client_implementation,
    meta_consensus_implementation,
    meta_consensus_version,
    meta_client_geo_city,
    meta_client_geo_country,
    meta_client_geo_country_code,
    block_version,
    block_total_bytes,
    block_total_bytes_compressed,
    execution_payload_value,
    consensus_payload_value,
    execution_payload_block_number,
    execution_payload_gas_limit,
    execution_payload_gas_used,
    execution_payload_transactions_count,
    execution_payload_transactions_total_bytes,
    meta_network_name
FROM `{{ index .dep "{{external}}" "beacon_api_eth_v3_validator_block" "database" }}`.`beacon_api_eth_v3_validator_block` FINAL
WHERE
    slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
ORDER BY slot_start_date_time, slot, meta_client_name, event_date_time
