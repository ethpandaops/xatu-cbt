ALTER TABLE dim_client_commit_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `base_tag` 'Release tag the commit builds on: the tag reachable in the fewest parent steps, as git describe picks it, empty before the first tag';

ALTER TABLE dim_client_commit ON CLUSTER '{cluster}'
    COMMENT COLUMN `base_tag` 'Release tag the commit builds on: the tag reachable in the fewest parent steps, as git describe picks it, empty before the first tag';

ALTER TABLE int_block_graffiti_release_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `consensus_client_version` 'Consensus client release version from dim_client_release (or the release a branch commit builds on), empty when unresolved';

ALTER TABLE int_block_graffiti_release_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Canonical blocks with the client releases their graffiti resolves to, resolved like the Lab slot page. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE int_block_graffiti_release ON CLUSTER '{cluster}'
    COMMENT COLUMN `consensus_client_version` 'Consensus client release version from dim_client_release (or the release a branch commit builds on), empty when unresolved';

ALTER TABLE int_block_graffiti_release ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Canonical blocks with the client releases their graffiti resolves to, resolved like the Lab slot page. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_block_graffiti_client_version_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `version` 'Release version the graffiti resolves to, empty when unresolved';

ALTER TABLE fct_block_graffiti_client_version_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly canonical block counts per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_block_graffiti_client_version_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `version` 'Release version the graffiti resolves to, empty when unresolved';

ALTER TABLE fct_block_graffiti_client_version_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly canonical block counts per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_block_graffiti_client_version_by_entity_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `version` 'Release version the graffiti resolves to, empty when unresolved';

ALTER TABLE fct_block_graffiti_client_version_by_entity_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly canonical block counts per staking entity, per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_block_graffiti_client_version_by_entity_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `version` 'Release version the graffiti resolves to, empty when unresolved';

ALTER TABLE fct_block_graffiti_client_version_by_entity_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly canonical block counts per staking entity, per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_block_graffiti_client_version_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `version` 'Release version the graffiti resolves to, empty when unresolved';

ALTER TABLE fct_block_graffiti_client_version_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily canonical block counts per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_block_graffiti_client_version_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `version` 'Release version the graffiti resolves to, empty when unresolved';

ALTER TABLE fct_block_graffiti_client_version_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily canonical block counts per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_block_graffiti_client_version_by_entity_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `version` 'Release version the graffiti resolves to, empty when unresolved';

ALTER TABLE fct_block_graffiti_client_version_by_entity_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily canonical block counts per staking entity, per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_block_graffiti_client_version_by_entity_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `version` 'Release version the graffiti resolves to, empty when unresolved';

ALTER TABLE fct_block_graffiti_client_version_by_entity_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily canonical block counts per staking entity, per consensus and execution client release resolved from block graffiti. Each block counts once per layer. release_status: commit_match (the graffiti commit is a release tag commit), commit_untagged (a default or release branch commit between tags, version is the release it builds on), commit_ambiguous (the prefix matches several commits), commit_unmatched (no release or branch commit, a custom or fork build), commit_short (a 2 character commit), commit_zero (an all-zero commit), version_match or version_unmatched (a version string with no commit, matched to a release tag or not), codes_min (client codes without commits), no_commit (no version information) or none (unknown client). Unresolved rows have an empty version';

ALTER TABLE fct_peer_count_by_cl_client_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_count_by_cl_client_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily consensus peer census by client and release. Consensus peers our libp2p sentries saw in the day (heartbeats and identify exchanges), one count per peer with its latest agent and address. Crawlers, probes, light clients and our own sentries are excluded. Custody group counts come from metadata responses to our outbound requests. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_count_by_cl_client_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_count_by_cl_client_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily consensus peer census by client and release. Consensus peers our libp2p sentries saw in the day (heartbeats and identify exchanges), one count per peer with its latest agent and address. Crawlers, probes, light clients and our own sentries are excluded. Custody group counts come from metadata responses to our outbound requests. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_count_by_el_client_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_count_by_el_client_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution node census by client and release. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_count_by_el_client_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_count_by_el_client_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution node census by client and release. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_fork_readiness_by_cl_client_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_fork_readiness_by_cl_client_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily consensus nodes from discovery (ENRs) by client, release, fork digest and next fork digest. A next fork digest of 0x00000000 or equal to the current digest means no next fork and is stored as empty. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_fork_readiness_by_cl_client_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_fork_readiness_by_cl_client_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily consensus nodes from discovery (ENRs) by client, release, fork digest and next fork digest. A next fork digest of 0x00000000 or equal to the current digest means no next fork and is stored as empty. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_fork_readiness_by_el_client_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_fork_readiness_by_el_client_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution nodes from discovery by client, release and EIP-2124 fork id. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). A next fork of 0 means none and is stored as empty. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_fork_readiness_by_el_client_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_peer_fork_readiness_by_el_client_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily execution nodes from discovery by client, release and EIP-2124 fork id. Execution nodes our discovery crawler found in the day, one count per node with its latest record, on the network genesis (the most common one that day, which excludes other chains). A next fork of 0 means none and is stored as empty. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_custody_probe_by_client_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_custody_probe_by_client_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily data column custody probe outcomes by the probed peer client and release (its latest agent in the day), from int_custody_probe. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_custody_probe_by_client_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `release_status` 'How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';

ALTER TABLE fct_custody_probe_by_client_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily data column custody probe outcomes by the probed peer client and release (its latest agent in the day), from int_custody_probe. How the version was resolved: commit_match (a release tag commit), commit_untagged (a default or release branch commit, version is the release it builds on), commit_ambiguous, commit_unmatched (a custom or fork build), commit_zero, version_match, version_unmatched or none';
