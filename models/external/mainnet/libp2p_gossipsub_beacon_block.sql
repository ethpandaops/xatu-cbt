---
database: mainnet
table: libp2p_gossipsub_beacon_block
cache:
  incremental_scan_interval: 5s
  full_scan_interval: 24h
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
-- Use the default database as predicate pushdown does not work with views.
-- This gives 2-3x the performance.
-- Once we move the data into the mainnet database, we no longer need this.
FROM `default`.`{{ .self.table }}`
WHERE 
    meta_network_name = 'mainnet'
{{ if .cache.is_incremental_scan }}
    AND (
      slot_start_date_time <= fromUnixTimestamp({{ .cache.previous_min }})
      OR slot_start_date_time >= fromUnixTimestamp({{ .cache.previous_max }})
    )
{{ end }}
