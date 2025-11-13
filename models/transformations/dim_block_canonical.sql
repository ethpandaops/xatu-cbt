---
table: dim_block_canonical
type: scheduled
schedule: "@every 5s"
tags:
  - block
  - canonical
dependencies:
  - "{{external}}.canonical_execution_block"
  - "{{external}}.canonical_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH latest_row AS (
  SELECT block_date_time FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL ORDER BY block_number DESC LIMIT 1
),

earliest_row AS (
  SELECT
    block_date_time
  FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
  WHERE
    meta_network_name = '{{ .env.NETWORK }}'
    -- ignore genesis
    AND block_number > 0
  ORDER BY block_number ASC LIMIT 1
),

reference_row AS (
  -- Use latest_row if exists, otherwise use earliest_row
  SELECT * FROM latest_row
  UNION ALL
  SELECT * FROM earliest_row WHERE (SELECT count(*) FROM latest_row) = 0
  LIMIT 1
),

-- Execution layer data
execution_blocks AS (
  SELECT
    block_number,
    block_hash,
    block_date_time
  FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
  WHERE
    block_date_time
      BETWEEN (SELECT block_date_time FROM reference_row) - INTERVAL 1 HOUR
      AND (SELECT block_date_time FROM reference_row) + INTERVAL 1 WEEK
    AND meta_network_name = '{{ .env.NETWORK }}'
),

-- Consensus layer data
post_merge_beacon_blocks AS (
  SELECT
    execution_payload_block_number AS block_number,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_version,
    block_root,
    parent_root,
    state_root
  FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
  WHERE
    slot_start_date_time
      BETWEEN (SELECT block_date_time FROM reference_row) - INTERVAL 1 HOUR
      AND (SELECT block_date_time FROM reference_row) + INTERVAL 1 WEEK
    AND meta_network_name = '{{ .env.NETWORK }}'
    -- after the merge
    AND block_version NOT IN ('phase0', 'altair')
)

SELECT
  ex.block_number AS execution_block_number,
  ex.block_hash AS execution_block_hash,
  ex.block_date_time AS execution_block_date_time,
  if(cs.block_number IS NOT NULL, cs.slot, NULL) AS slot,
  if(cs.block_number IS NOT NULL, cs.slot_start_date_time, NULL) AS slot_start_date_time,
  if(cs.block_number IS NOT NULL, cs.epoch, NULL) AS epoch,
  if(cs.block_number IS NOT NULL, cs.epoch_start_date_time, NULL) AS epoch_start_date_time,
  if(cs.block_number IS NOT NULL, cs.block_version, NULL) AS beacon_block_version,
  if(cs.block_number IS NOT NULL, cs.block_root, NULL) AS beacon_block_root,
  if(cs.block_number IS NOT NULL, cs.parent_root, NULL) AS beacon_parent_root,
  if(cs.block_number IS NOT NULL, cs.state_root, NULL) AS beacon_state_root
FROM execution_blocks ex
LEFT JOIN post_merge_beacon_blocks cs ON ex.block_number = cs.block_number;
