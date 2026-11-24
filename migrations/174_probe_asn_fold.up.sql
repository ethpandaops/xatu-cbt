-- fct_custody_probe_by_asn_daily now rolls residential autonomous systems with under 3 probed peers in the day into
-- autonomous system 0 (other), like the other autonomous system rollups, so no row can be tied back to one peer.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE fct_custody_probe_by_asn_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily data column custody probes by the probed peer autonomous system (its latest location in the day), residential and hosting. Residential autonomous systems with under 3 probed peers in the day roll into autonomous system 0 (other). Crawlers and probes are excluded.';

ALTER TABLE fct_custody_probe_by_asn_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `autonomous_system_number` 'Autonomous system number of the address, 0 when unknown and for residential autonomous systems with under 3 probed peers in the day (other)',
    COMMENT COLUMN `autonomous_system_organization` 'Autonomous system organisation, unknown when not geolocated, other for residential autonomous systems with under 3 probed peers in the day';

ALTER TABLE fct_custody_probe_by_asn_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily data column custody probes by the probed peer autonomous system (its latest location in the day), residential and hosting. Residential autonomous systems with under 3 probed peers in the day roll into autonomous system 0 (other). Crawlers and probes are excluded.';

ALTER TABLE fct_custody_probe_by_asn_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `autonomous_system_number` 'Autonomous system number of the address, 0 when unknown and for residential autonomous systems with under 3 probed peers in the day (other)',
    COMMENT COLUMN `autonomous_system_organization` 'Autonomous system organisation, unknown when not geolocated, other for residential autonomous systems with under 3 probed peers in the day';
