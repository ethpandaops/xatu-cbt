---
table: int_post_6780_accounts_destructs
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
  - selfdestruct
  - eip6780
dependencies:
  - "{{external}}.canonical_execution_address_appearances"
  - "{{external}}.canonical_execution_transaction"
---
-- This model tracks self-destruct operations that occurred after EIP-6780 (Dencun fork)
-- EIP-6780 changed SELFDESTRUCT behavior at block 19426587 on mainnet
-- After EIP-6780, SELFDESTRUCT only deletes the account if it was created in the same transaction
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH 
get_tx_success AS (
  SELECT
    lower(transaction_hash) AS transaction_hash,
    transaction_index
  FROM `{{ index .dep "{{external}}" "canonical_execution_transaction" "database" }}`.`canonical_execution_transaction` FINAL
  WHERE
    block_number >= 19426587  -- EIP-6780 block
    AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND success = true
),
address_events AS (
  SELECT
    lower(address) AS address,
    block_number,
    lower(transaction_hash) AS transaction_hash,
    max(CASE WHEN relationship = 'create' THEN 1 ELSE 0 END) AS has_create,
    max(CASE WHEN relationship = 'suicide' THEN 1 ELSE 0 END) AS has_suicide
  FROM `{{ index .dep "{{external}}" "canonical_execution_address_appearances" "database" }}`.`canonical_execution_address_appearances` FINAL
  WHERE
    block_number >= 19426587  -- EIP-6780 block
    AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND relationship IN ('create', 'suicide')
  GROUP BY 
    address,
    block_number,
    transaction_hash
)
SELECT
  ae.address,
  ae.block_number,
  ae.transaction_hash,
  g.transaction_index,
  CASE 
    WHEN ae.has_create = 1 AND ae.has_suicide = 1 THEN true
    ELSE false
  END AS is_same_tx
FROM address_events ae
GLOBAL JOIN get_tx_success g
  ON ae.transaction_hash = g.transaction_hash
WHERE ae.has_suicide = 1;
