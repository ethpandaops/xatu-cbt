---
table: dim_block
type: scheduled
schedule: "@every 5s"
tags:
  - block
  - canonical
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH latest_row AS (
  SELECT slot_start_date_time FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL ORDER BY slot_start_date_time DESC LIMIT 1
),

earliest_row AS (
  SELECT slot_start_date_time FROM cluster('{remote_cluster}', default.`canonical_beacon_block`) FINAL ORDER BY slot_start_date_time ASC LIMIT 1
),

reference_row AS (
  -- Use latest_row if exists, otherwise use earliest_row
  SELECT * FROM latest_row
  UNION ALL
  SELECT * FROM earliest_row WHERE (SELECT count(*) FROM latest_row) = 0
  LIMIT 1
),

blocks AS (
  SELECT
    execution_payload_block_number AS block_number,
    execution_payload_block_hash AS block_hash,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    parent_root,
    state_root
  FROM cluster('{remote_cluster}', default.`canonical_beacon_block`) FINAL
  WHERE 
    slot_start_date_time
      BETWEEN (SELECT slot_start_date_time FROM reference_row) - INTERVAL 1 HOUR
      AND (SELECT slot_start_date_time FROM reference_row) + INTERVAL 1 WEEK
    AND execution_payload_block_number IS NOT NULL
    AND execution_payload_block_hash IS NOT NULL
)

SELECT
  block_number,
  block_hash,
  slot,
  slot_start_date_time,
  epoch,
  epoch_start_date_time,
  block_root,
  parent_root,
  state_root
FROM blocks;
