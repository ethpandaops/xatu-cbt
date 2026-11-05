ALTER TABLE int_custody_probe_local ON CLUSTER '{cluster}'
    DROP INDEX IF EXISTS idx_peer_id_unique_key;

ALTER TABLE int_custody_probe_local ON CLUSTER '{cluster}'
    DROP INDEX IF EXISTS idx_node_id;

ALTER TABLE int_custody_probe_order_by_slot_local ON CLUSTER '{cluster}'
    DROP INDEX IF EXISTS idx_peer_id_unique_key;

ALTER TABLE int_custody_probe_order_by_slot_local ON CLUSTER '{cluster}'
    DROP INDEX IF EXISTS idx_node_id;
