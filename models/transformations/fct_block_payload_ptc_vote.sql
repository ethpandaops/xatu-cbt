---
table: fct_block_payload_ptc_vote
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - slot
  - payload
  - ptc
  - epbs
  - canonical
dependencies:
  - "{{transformation}}.int_block_payload_ptc_vote_canonical"
  - "{{transformation}}.fct_block_payload_ptc_vote_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas (ePBS): per-block PTC verdict. Canonical rows carry the on-chain
-- aggregated vote counts; blocks only ever seen on the live stream are tagged
-- orphaned with the stream-observed counts.
WITH canonical_votes AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        block_version,
        included_in_slot,
        included_in_block_root,
        ptc_validators,
        payload_present_votes,
        blob_data_available_votes,
        'canonical' AS `status`
    FROM {{ index .dep "{{transformation}}" "int_block_payload_ptc_vote_canonical" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
orphaned_votes AS (
    SELECT
        h.slot AS slot,
        h.slot_start_date_time AS slot_start_date_time,
        h.epoch AS epoch,
        h.epoch_start_date_time AS epoch_start_date_time,
        h.block_root AS block_root,
        '' AS block_version,
        NULL AS included_in_slot,
        NULL AS included_in_block_root,
        h.ptc_validators_seen AS ptc_validators,
        h.payload_present_votes AS payload_present_votes,
        h.blob_data_available_votes AS blob_data_available_votes,
        'orphaned' AS `status`
    FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote_head" "helpers" "from" }} AS h FINAL
    GLOBAL LEFT ANTI JOIN canonical_votes c
        ON h.slot_start_date_time = c.slot_start_date_time
        AND h.block_root = c.block_root
    WHERE h.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    *
FROM (
    SELECT * FROM canonical_votes
    UNION ALL
    SELECT * FROM orphaned_votes
)
