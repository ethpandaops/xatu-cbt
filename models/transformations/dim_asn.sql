---
table: dim_asn
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 1m"
tags:
  - peer
  - geo
dependencies:
  - "{{transformation}}.fct_peer_count_by_asn_daily"
---
-- One row per autonomous system our consensus peers were on, residential and hosting, with its organisation name,
-- provider and network type on the last day it had peers. Each run writes the systems seen in the days its bounds
-- touch, versioned by that day, so the latest day wins whatever order days are processed in.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    autonomous_system_number,
    latest.1 AS autonomous_system_organization,
    -- Providers that span several networks or organisation names get one name, the rest drop legal suffixes.
    multiIf(
        autonomous_system_organization IN ('', 'unknown'), '',
        match(autonomous_system_organization, '(?i)^amazon'), 'Amazon',
        match(autonomous_system_organization, '(?i)^google (llc|cloud)'), 'Google',
        match(autonomous_system_organization, '(?i)^microsoft'), 'Microsoft',
        match(autonomous_system_organization, '(?i)alibaba'), 'Alibaba',
        match(autonomous_system_organization, '(?i)^oracle'), 'Oracle',
        match(autonomous_system_organization, '(?i)^ovh'), 'OVHcloud',
        match(autonomous_system_organization, '(?i)^hetzner'), 'Hetzner',
        match(autonomous_system_organization, '(?i)^contabo'), 'Contabo',
        match(autonomous_system_organization, '(?i)^(akamai|linode)'), 'Akamai',
        match(autonomous_system_organization, '(?i)^tencent'), 'Tencent',
        trimRight(
            replaceRegexpOne(
                replaceRegexpOne(autonomous_system_organization, '(?i)[,.]?\\s+(inc|llc|l\\.l\\.c|ltd|ltda|limited|private limited|pte|pty|gmbh|corp|corporation|co|company|sas|sa|s\\.a|s\\.a\\.u|bv|b\\.v|a\\.s|aps|ag|plc|oy|ab|srl|ltee|sp\\. z o\\.o)\\.?$', ''),
                '(?i)[,.]?\\s+(inc|llc|ltd|limited|private|pte|pty|gmbh|corp|corporation|co|company|sas|sa|s\\.a|bv|b\\.v|ag|plc)\\.?$', ''
            ),
            ' ,.'
        )
    ) AS provider,
    latest.2 AS network_type,
    last_seen_date
FROM (
    SELECT
        autonomous_system_number,
        max(day_start_date) AS last_seen_date,
        argMax((autonomous_system_organization, network_type), (day_start_date, peer_count, autonomous_system_organization)) AS latest
    FROM {{ index .dep "{{transformation}}" "fct_peer_count_by_asn_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= toDate(fromUnixTimestamp({{ .bounds.start }}))
      AND day_start_date <= toDate(fromUnixTimestamp({{ .bounds.end }}))
      AND autonomous_system_number != 0
      AND peer_count > 0
    GROUP BY autonomous_system_number
)
SETTINGS parallel_distributed_insert_select = 0
