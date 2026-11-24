-- Skip indexes for the probe filters Lab sends on int_custody_probe and
-- int_custody_probe_order_by_slot (peer_id_unique_key_in_values and node_id_eq).
--
-- Both filters match values that only exist for part of the table's history
-- (peers and probing nodes come and go), so most granules can be skipped. The
-- other probe filters (result, meta_peer_implementation, meta_client_implementation,
-- column_indices) match values present in nearly every 8192-row granule, so an
-- index on them would never skip anything and is not added.
--
-- Indexes live on the _local tables only. MATERIALIZE INDEX is an async mutation
-- that builds the index for existing parts by reading just the indexed columns.

ALTER TABLE int_custody_probe_local ON CLUSTER '{cluster}'
    ADD INDEX IF NOT EXISTS idx_peer_id_unique_key peer_id_unique_key TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE int_custody_probe_local ON CLUSTER '{cluster}'
    ADD INDEX IF NOT EXISTS idx_node_id node_id TYPE set(100) GRANULARITY 1;

ALTER TABLE int_custody_probe_local ON CLUSTER '{cluster}'
    MATERIALIZE INDEX idx_peer_id_unique_key;

ALTER TABLE int_custody_probe_local ON CLUSTER '{cluster}'
    MATERIALIZE INDEX idx_node_id;

ALTER TABLE int_custody_probe_order_by_slot_local ON CLUSTER '{cluster}'
    ADD INDEX IF NOT EXISTS idx_peer_id_unique_key peer_id_unique_key TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE int_custody_probe_order_by_slot_local ON CLUSTER '{cluster}'
    ADD INDEX IF NOT EXISTS idx_node_id node_id TYPE set(100) GRANULARITY 1;

ALTER TABLE int_custody_probe_order_by_slot_local ON CLUSTER '{cluster}'
    MATERIALIZE INDEX idx_peer_id_unique_key;

ALTER TABLE int_custody_probe_order_by_slot_local ON CLUSTER '{cluster}'
    MATERIALIZE INDEX idx_node_id;
