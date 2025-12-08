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
tags:
  - execution
dependencies:
  - "{{external}}.canonical_execution_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_date_time,
    block_date_time,
    block_number
FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} AS eb FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
