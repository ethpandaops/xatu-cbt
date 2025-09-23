---
table: fct_block_blob_count
interval:
  max: 50000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - block
  - canonical
dependencies:
  - "{{transformation}}.int_block_blob_count_canonical"
  - "{{transformation}}.fct_block_blob_count_head"
  # TODO: this is broken as int_block_blob_count_canonical has the wrong block_root
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH canonical_blocks AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        blob_count,
        'canonical' AS `status`
    FROM `{{ index .dep "{{transformation}}" "int_block_blob_count_canonical" "database" }}`.`int_block_blob_count_canonical` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
orphaned_blocks AS (
    SELECT
        h.slot AS slot,
        h.slot_start_date_time AS slot_start_date_time,
        h.epoch AS epoch,
        h.epoch_start_date_time AS epoch_start_date_time,
        h.block_root AS block_root,
        h.blob_count AS blob_count,
        'orphaned' AS `status`
    FROM `{{ index .dep "{{transformation}}" "fct_block_blob_count_head" "database" }}`.`fct_block_blob_count_head` AS h FINAL
    GLOBAL LEFT ANTI JOIN canonical_blocks c 
        ON h.slot_start_date_time = c.slot_start_date_time 
        AND h.block_root = c.block_root
    WHERE h.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    *
FROM (
    SELECT * FROM canonical_blocks
    UNION ALL
    SELECT * FROM orphaned_blocks
)
