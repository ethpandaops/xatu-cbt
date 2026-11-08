ALTER TABLE fct_peer_count_by_el_client_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution node census by client and release. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). How the version was resolved: commit_match (a release tag commit agreeing with any stated version), commit_untagged (a default or release branch commit, version is the release it builds on, else the stated version), commit_mismatch (the commit resolves to another release than the stated version, version is the commit release), commit_ambiguous, commit_unmatched (a custom or fork build, the stated version is kept), commit_zero, version_match, version_unmatched or none. Rows whose commit does not resolve keep the version the agent states';

ALTER TABLE fct_peer_count_by_el_client_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution node census by client and release. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). How the version was resolved: commit_match (a release tag commit agreeing with any stated version), commit_untagged (a default or release branch commit, version is the release it builds on, else the stated version), commit_mismatch (the commit resolves to another release than the stated version, version is the commit release), commit_ambiguous, commit_unmatched (a custom or fork build, the stated version is kept), commit_zero, version_match, version_unmatched or none. Rows whose commit does not resolve keep the version the agent states';

ALTER TABLE fct_peer_count_by_el_client_country_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution node census by client and country. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). Countries under 3 nodes roll into their continent other bucket, and continents still under 3 into a global other bucket.';

ALTER TABLE fct_peer_count_by_el_client_country_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution node census by client and country. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). Countries under 3 nodes roll into their continent other bucket, and continents still under 3 into a global other bucket.';

ALTER TABLE fct_peer_count_by_el_client_asn_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution node census by client and autonomous system. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). Autonomous systems under 3 nodes roll into an other bucket per network type.';

ALTER TABLE fct_peer_count_by_el_client_asn_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution node census by client and autonomous system. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). Autonomous systems under 3 nodes roll into an other bucket per network type.';

ALTER TABLE fct_peer_count_by_client_pair_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily consensus and execution client pairs from nodes sharing an IPv4 address: consensus peers from libp2p and ENRs, execution nodes from discovery. Only addresses with exactly one consensus and one execution client form a pair. No address is stored.';

ALTER TABLE fct_peer_count_by_client_pair_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily consensus and execution client pairs from nodes sharing an IPv4 address: consensus peers from libp2p and ENRs, execution nodes from discovery. Only addresses with exactly one consensus and one execution client form a pair. No address is stored.';
