---
table: fct_peer_custody_count_by_epoch
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - epoch
  - peerdas
  - custody
  - peer
dependencies:
  - "{{transformation}}.int_peer_custody_count_by_epoch"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    src.epoch,
    src.epoch_start_date_time,
    src.custody_group_count,
    toUInt32(countDistinct(src.peer_id_unique_key)) AS peer_count
FROM {{ index .dep "{{transformation}}" "int_peer_custody_count_by_epoch" "helpers" "from" }} AS src FINAL
WHERE
    src.epoch_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY
    src.epoch,
    src.epoch_start_date_time,
    src.custody_group_count
