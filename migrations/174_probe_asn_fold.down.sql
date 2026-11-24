-- Restore the comments set in 173_network_peer_geo.up.sql.

ALTER TABLE fct_custody_probe_by_asn_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily data column custody probes by the probed peer autonomous system (its latest location in the day), residential and hosting. Crawlers and probes are excluded.';

ALTER TABLE fct_custody_probe_by_asn_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `autonomous_system_number` 'Autonomous system number of the address, 0 when unknown',
    COMMENT COLUMN `autonomous_system_organization` 'Autonomous system organisation, unknown when not geolocated';

ALTER TABLE fct_custody_probe_by_asn_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily data column custody probes by the probed peer autonomous system (its latest location in the day), residential and hosting. Crawlers and probes are excluded.';

ALTER TABLE fct_custody_probe_by_asn_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `autonomous_system_number` 'Autonomous system number of the address, 0 when unknown',
    COMMENT COLUMN `autonomous_system_organization` 'Autonomous system organisation, unknown when not geolocated';
