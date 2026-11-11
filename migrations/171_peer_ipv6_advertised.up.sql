-- Our sentries connect to peers over IPv4 only, so the address they connected to never showed IPv6 and ipv6 was
-- always false. int_peer_daily now marks a peer IPv6 when it advertises a global IPv6 address: an identify listen
-- address or its ENR record.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE int_peer_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6` 'The peer advertises a global IPv6 address, in an identify listen address or its ENR record, or we connected to it over IPv6';

ALTER TABLE int_peer_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6` 'The peer advertises a global IPv6 address, in an identify listen address or its ENR record, or we connected to it over IPv6';

ALTER TABLE int_peer_daily_order_by_peer_id_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6` 'The peer advertises a global IPv6 address, in an identify listen address or its ENR record, or we connected to it over IPv6';

ALTER TABLE int_peer_daily_order_by_peer_id ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6` 'The peer advertises a global IPv6 address, in an identify listen address or its ENR record, or we connected to it over IPv6';

ALTER TABLE fct_peer_count_by_source_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6_peer_count` 'Peers that advertise a global IPv6 address, in an identify listen address or their ENR record';

ALTER TABLE fct_peer_count_by_source_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `ipv6_peer_count` 'Peers that advertise a global IPv6 address, in an identify listen address or their ENR record';
