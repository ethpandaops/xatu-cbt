---
table: int_backfill
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
dependencies:
  - "{{external}}.libp2p_rpc_data_column_custody_probe"
  - "{{external}}.libp2p_synthetic_heartbeat"
---
SELECT 1
