---
table: int_execution_block_by_date
type: incremental
interval:
  type: block
  max: 500000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - execution
dependencies:
  - "{{external}}.canonical_execution_block"
---
-- Block number to block time, for every canonical block of this network.
--
-- canonical_execution_block only reports its min and max block, and Cannon can still be writing blocks in
-- between, e.g. while it backfills history. CBT never goes back to a range it has processed, so a range read
-- before its blocks land would stay short for good. This first statement fails the task, and CBT retries it,
-- until every block in the range is present.
SELECT throwIf(
    (
        SELECT uniqExact(block_number)
        FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            AND meta_network_name = '{{ .env.NETWORK }}'
    ) < {{ .bounds.end }} - {{ .bounds.start }} + 1,
    'canonical_execution_block is missing blocks in this range, waiting for Cannon to write them'
);

INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_date_time,
    block_number
FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
