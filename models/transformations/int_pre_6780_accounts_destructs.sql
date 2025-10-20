---
table: int_pre_6780_accounts_destructs
type: incremental
interval:
  type: block
  max: 10000
schedules:
  forwardfill: "off"
  backfill: "@every 1m"
tags:
  - address
  - account
  - selfdestruct
dependencies:
  - "{{external}}.canonical_execution_traces"
  - "{{external}}.canonical_execution_transaction"
---
-- This model tracks self-destruct operations that occurred before EIP-6780 (Dencun fork)
-- EIP-6780 changed SELFDESTRUCT behavior at block 19426587 on mainnet
-- Pre-EIP161 (Spurious Dragon, block 2675000) empty accounts require special handling
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH 
get_tx_success AS (
  SELECT
    lower(transaction_hash) AS transaction_hash,
    transaction_index
  FROM `{{ index .dep "{{external}}" "canonical_execution_transaction" "database" }}`.`canonical_execution_transaction` FINAL
  WHERE
    block_number < 19426587  -- EIP-6780 block
    AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND success = true
),
-- Before EIP161 was activated, there was a DoS attack by calling SELFDESTRUCT on contracts 
-- and sending 0 ETH to newly seen addresses, which results in empty accounts created.
pre_eip161_empty_accounts AS (
  SELECT 
    lower(action_to) AS address,
    block_number,
    lower(transaction_hash) AS tx_hash
  FROM `{{ index .dep "{{external}}" "canonical_execution_traces" "database" }}`.`canonical_execution_traces` FINAL
  WHERE
    action_type = 'suicide'
    AND block_number < 2675000  -- EIP-161 block
    AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND action_value = '0'
),
self_destructs AS (
  SELECT
    lower(action_from) AS address,
    block_number,
    lower(transaction_hash) AS tx_hash
  FROM `{{ index .dep "{{external}}" "canonical_execution_traces" "database" }}`.`canonical_execution_traces` FINAL
  WHERE
    action_type = 'suicide'
    AND block_number < 19426587  -- EIP-6780 block
    AND block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
  
  UNION ALL
  
  SELECT address, block_number, tx_hash 
  FROM pre_eip161_empty_accounts
)
SELECT
  s.address,
  s.block_number,
  s.tx_hash AS transaction_hash,
  max(g.transaction_index) AS transaction_index
FROM self_destructs s
GLOBAL JOIN get_tx_success g ON s.tx_hash = g.transaction_hash
GROUP BY s.address, s.block_number, s.tx_hash;
