---
table: int_accounts_alive
type: incremental
interval:
  type: block
  max: 10000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - address
  - account
  - alive
dependencies:
  - "{{transformation}}.int_address_diffs"
  - "{{transformation}}.int_pre_6780_accounts_destructs"
  - "{{transformation}}.int_post_6780_accounts_destructs"
---
-- This model tracks whether accounts are alive or dead based on all events
-- Combines diffs and destructs to determine the latest status using argMax
-- After EIP-6780, self-destructs only kill accounts if created in same transaction
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Pre-6780 destructs: always mark as dead
pre_6780_destructs AS (
  SELECT
    address,
    block_number AS block_num,
    transaction_index,
    false AS is_alive
  FROM `{{ index .dep "{{transformation}}" "int_pre_6780_accounts_destructs" "database" }}`.`int_pre_6780_accounts_destructs`
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Post-6780 destructs: dead only if is_same_tx = true
post_6780_destructs AS (
  SELECT
    address,
    block_number AS block_num,
    transaction_index,
    CASE 
      WHEN is_same_tx = true THEN false  -- Account actually destroyed
      ELSE true                           -- Account not destroyed, just cleared
    END AS is_alive
  FROM `{{ index .dep "{{transformation}}" "int_post_6780_accounts_destructs" "database" }}`.`int_post_6780_accounts_destructs`
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Diffs: always mark as alive (use existing last_tx_index column)
diffs AS (
  SELECT
    address,
    block_number AS block_num,
    last_tx_index AS transaction_index,
    true AS is_alive
  FROM `{{ index .dep "{{transformation}}" "int_address_diffs" "database" }}`.`int_address_diffs`
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
),
-- Combine all events
combined AS (
  SELECT * FROM pre_6780_destructs
  UNION ALL
  SELECT * FROM post_6780_destructs
  UNION ALL
  SELECT * FROM diffs
)
-- Get the latest status for each address
-- When there are ties (same block_number, transaction_index), pessimistically choose false
SELECT
  address,
  max(block_num) AS block_number,
  argMax(is_alive, (block_num, transaction_index, NOT is_alive)) AS is_alive
FROM combined
GROUP BY address;
