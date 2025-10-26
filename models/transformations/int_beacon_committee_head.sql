---
table: int_beacon_committee_head
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - slot
  - beacon
  - committee
  - head
dependencies:
  - "{{external}}.beacon_api_eth_v1_beacon_committee"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
per_view AS
(
    SELECT DISTINCT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        committee_index,
        arraySort(arrayDistinct(validators)) AS v_norm,
        (meta_client_name, meta_client_id)   AS client
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_committee" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

votes AS
(
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        committee_index,
        v_norm,
        uniqExact(client) AS votes
    FROM per_view
    GROUP BY
        slot, slot_start_date_time, epoch, epoch_start_date_time,
        committee_index, v_norm
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    committee_index,
    argMax(v_norm, (votes, toUInt64(cityHash64(v_norm)))) AS validators
FROM votes
GROUP BY
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    committee_index
ORDER BY slot, committee_index;
