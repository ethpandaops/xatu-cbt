---
table: fct_attestation_vote_by_block_root
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - attestation
  - canonical
dependencies:
  - "{{transformation}}.int_attestation_attested"
---
-- Head votes per slot and voted block root, from the merged canonical-preferred attestations in int_attestation_attested.
-- int_attestation_attested has one row per (slot, block root, validator), so the count is the number of validators.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    toUInt32(count()) AS vote_count
FROM {{ index .dep "{{transformation}}" "int_attestation_attested" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
