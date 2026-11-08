-- Restore the comments set in 169_network_peer_daily.up.sql.

ALTER TABLE int_peer_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6` 'The address is IPv6';

ALTER TABLE int_peer_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6` 'The address is IPv6';

ALTER TABLE int_peer_daily_order_by_peer_id_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6` 'The address is IPv6';

ALTER TABLE int_peer_daily_order_by_peer_id ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6` 'The address is IPv6';

ALTER TABLE fct_peer_count_by_source_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6_peer_count` 'Peers on an IPv6 address';

ALTER TABLE fct_peer_count_by_source_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6_peer_count` 'Peers on an IPv6 address';
