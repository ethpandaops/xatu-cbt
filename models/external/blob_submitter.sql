---
table: blob_submitter
cache:
  incremental_scan_interval: 24h
  full_scan_interval: 48h
interval:
  type: address
---
SELECT 
    0 as min,
    0 as max
