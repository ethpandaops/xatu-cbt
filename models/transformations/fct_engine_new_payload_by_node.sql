---
table: fct_engine_new_payload_by_node
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - engine_api
  - new_payload
dependencies:
  - "{{external}}.consensus_engine_api_new_payload"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    parent_block_root,
    proposer_index,
    block_number,
    block_hash,
    parent_hash,
    gas_used,
    gas_limit,
    tx_count,
    blob_count,
    status,
    latest_valid_hash,
    validation_error,
    duration_ms,
    method_version,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN
            splitByChar('/', meta_client_name)[2]
        WHEN startsWith(meta_client_name, 'corp-') THEN
            splitByChar('/', meta_client_name)[2]
        ELSE
            'ethpandaops'
    END AS username,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN
            splitByChar('/', meta_client_name)[3]
        WHEN startsWith(meta_client_name, 'corp-') THEN
            splitByChar('/', meta_client_name)[3]
        ELSE
            splitByChar('/', meta_client_name)[-1]
    END AS node_id,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN
            'individual'
        WHEN startsWith(meta_client_name, 'corp-') THEN
            'corporate'
        WHEN startsWith(meta_client_name, 'ethpandaops') THEN
            'internal'
        ELSE
            'unclassified'
    END AS classification,
    meta_client_name,
    meta_client_version,
    meta_client_implementation,
    meta_execution_version,
    meta_execution_implementation,
    meta_execution_version_major,
    meta_execution_version_minor,
    meta_execution_version_patch,
    meta_client_geo_city,
    meta_client_geo_country,
    meta_client_geo_country_code,
    meta_client_geo_continent_code,
    meta_client_geo_longitude,
    meta_client_geo_latitude,
    meta_client_geo_autonomous_system_number,
    meta_client_geo_autonomous_system_organization
FROM {{ index .dep "{{external}}" "consensus_engine_api_new_payload" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND positionCaseInsensitive(meta_client_name, '7870') > 0
