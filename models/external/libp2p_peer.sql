---
table: libp2p_peer
cache:
  incremental_scan_interval: 24h
  full_scan_interval: 48h
interval:
  type: slot
---
-- Maps peer_id_unique_key (a hash of the peer ID and network) back to the peer ID, one row per peer. It is a lookup,
-- not a time series: models read only the keys they need, so the bounds are left empty and the table is never
-- scanned for them. Every model pairs it with heartbeats in an OR group, which skips members with empty bounds.
SELECT
    0 as min,
    0 as max
