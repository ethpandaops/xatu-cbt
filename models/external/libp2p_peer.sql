---
table: libp2p_peer
cache:
  incremental_scan_interval: 5s
  full_scan_interval: 24h
interval:
  type: slot
lag: 12
---
-- Maps peer_id_unique_key (a hash of the peer ID and network) back to the peer ID. One row per peer, refreshed
-- whenever the peer is seen again, so the bounds are the update times.
SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
    {{ else }}
      toUnixTimestamp(min(updated_date_time)) as min,
    {{ end }}
    toUnixTimestamp(max(updated_date_time)) as max
FROM {{ .self.helpers.from }}
WHERE
    meta_network_name = '{{ .env.NETWORK }}'
