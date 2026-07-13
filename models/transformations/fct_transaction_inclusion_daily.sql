---
table: fct_transaction_inclusion_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - transaction
  - execution
  - canonical
dependencies:
  - "{{transformation}}.fct_transaction_inclusion"
  - "{{transformation}}.fct_block_mev"
---
-- Daily counts of transactions included in canonical blocks, split by build path
-- and transaction type. The scan expands to complete day boundaries so partial
-- boundary days from a previous run get re-aggregated and replaced.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH txs AS (
    SELECT
        slot_start_date_time,
        block_root,
        type,
        blob_hashes,
        `to`,
        `from`,
        is_cancel_shape
    FROM {{ index .dep "{{transformation}}" "fct_transaction_inclusion" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 25 HOUR
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 25 HOUR
        AND toDate(slot_start_date_time) >= toDate(fromUnixTimestamp({{ .bounds.start }}))
        AND toDate(slot_start_date_time) <= toDate(fromUnixTimestamp({{ .bounds.end }}))
),
mev AS (
    SELECT DISTINCT
        slot_start_date_time,
        block_root
    FROM {{ index .dep "{{transformation}}" "fct_block_mev" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 25 HOUR
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 25 HOUR
        AND status = 'canonical'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(t.slot_start_date_time) AS day_start_date,
    count() AS included_count,
    countIf(m.block_root IS NOT NULL) AS relay_delivered_count,
    countIf(m.block_root IS NULL) AS unknown_build_count,
    countIf(t.type = 0) AS type_0_count,
    countIf(t.type = 1) AS type_1_count,
    countIf(t.type = 2) AS type_2_count,
    countIf(t.type = 3) AS type_3_count,
    countIf(t.type = 4) AS type_4_count,
    countIf(t.type > 4) AS type_other_count,
    sum(length(t.blob_hashes)) AS blob_count,
    countIf(t.`to` IS NULL) AS contract_creation_count,
    countIf(t.is_cancel_shape) AS cancel_shape_count,
    uniqExact(t.`from`) AS unique_senders
FROM txs t
GLOBAL LEFT JOIN mev m
    ON m.slot_start_date_time = t.slot_start_date_time AND m.block_root = t.block_root
GROUP BY day_start_date
SETTINGS join_use_nulls = 1
