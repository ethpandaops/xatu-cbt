---
table: dim_validator_pubkey
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - validator
  - pubkey
  - validator_performance
dependencies:
  - "{{external}}.canonical_beacon_validators_pubkeys"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    `index` AS validator_index,
    argMin(pubkey, epoch) AS pubkey
FROM {{ index .dep "{{external}}" "canonical_beacon_validators_pubkeys" "helpers" "from" }} FINAL
WHERE epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY `index`
