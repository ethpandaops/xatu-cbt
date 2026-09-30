---
table: fct_prepared_block
type: incremental
interval:
  type: slot
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
    -- Gloas (ePBS): a produced block commits to a payload bid instead of
    -- embedding the payload, so the sentry cannot see what it holds and the raw
    -- event reports zero transactions. NULL these rather than report an empty
    -- block for every gloas proposal.
    if(block_version IN ('phase0', 'altair', 'bellatrix', 'capella', 'deneb', 'electra', 'fulu'), execution_payload_gas_limit, NULL) AS execution_payload_gas_limit,
    if(block_version IN ('phase0', 'altair', 'bellatrix', 'capella', 'deneb', 'electra', 'fulu'), execution_payload_gas_used, NULL) AS execution_payload_gas_used,
    if(block_version IN ('phase0', 'altair', 'bellatrix', 'capella', 'deneb', 'electra', 'fulu'), execution_payload_transactions_count, NULL) AS execution_payload_transactions_count,
    if(block_version IN ('phase0', 'altair', 'bellatrix', 'capella', 'deneb', 'electra', 'fulu'), execution_payload_transactions_total_bytes, NULL) AS execution_payload_transactions_total_bytes
FROM {{ index .dep "{{external}}" "beacon_api_eth_v3_validator_block" "helpers" "from" }} FINAL
WHERE
    slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
ORDER BY slot_start_date_time, slot, meta_client_name, event_date_time
