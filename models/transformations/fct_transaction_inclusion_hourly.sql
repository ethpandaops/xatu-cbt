---
table: fct_transaction_inclusion_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - transaction
  - execution
  - canonical
dependencies:
  - "{{transformation}}.fct_transaction_inclusion"
  - "{{transformation}}.fct_block_mev"
---
-- Hourly counts of transactions included in canonical blocks, split by build path
-- and transaction type. The scan expands to complete hour boundaries so partial
-- boundary hours from a previous run get re-aggregated and replaced.
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
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
        AND toStartOfHour(slot_start_date_time) >= toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
        AND toStartOfHour(slot_start_date_time) <= toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
),
mev AS (
    SELECT DISTINCT
        slot_start_date_time,
        block_root
    FROM {{ index .dep "{{transformation}}" "fct_block_mev" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
        AND status = 'canonical'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(t.slot_start_date_time) AS hour_start_date_time,
    count() AS included_count,
    countIf(m.block_root IS NOT NULL) AS relay_delivered_count,
    countIf(m.block_root IS NULL) AS unknown_build_count,
    countIf(t.type = 0) AS type0_count,
    countIf(t.type = 1) AS type1_count,
    countIf(t.type = 2) AS type2_count,
    countIf(t.type = 3) AS type3_count,
    countIf(t.type = 4) AS type4_count,
    countIf(t.type > 4) AS type_other_count,
    sum(length(t.blob_hashes)) AS blob_count,
    countIf(t.`to` IS NULL) AS contract_creation_count,
    countIf(t.is_cancel_shape) AS cancel_shape_count,
    uniqExact(t.`from`) AS unique_senders
FROM txs t
GLOBAL LEFT JOIN mev m
    ON m.slot_start_date_time = t.slot_start_date_time AND m.block_root = t.block_root
GROUP BY hour_start_date_time
SETTINGS join_use_nulls = 1
