---
table: mev_relay_proposer_payload_delivered
cache:
  incremental_scan_interval: 5s
  full_scan_interval: 24h
interval:
  type: slot
lag: 12
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM {{ .self.helpers.from }}
WHERE 
    meta_network_name = '{{ .env.NETWORK }}'
{{ if .cache.is_incremental_scan }}
    AND (
      slot_start_date_time <= fromUnixTimestamp({{ .cache.previous_min }})
      OR slot_start_date_time >= fromUnixTimestamp({{ .cache.previous_max }})
    )
{{ end }}
