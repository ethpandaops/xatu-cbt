---
table: fct_attestation_correctness_head
interval:
  max: 384
schedules:
  forwardfill: "@every 5s"
tags:
  - slot
  - attestation
  - head
dependencies:
  - "{{transformation}}.int_attestation_attested_head"
  - "{{transformation}}.int_block_first_seen_by_node"
  - "{{transformation}}.int_attestation_first_seen"
  - "{{transformation}}.int_block_blob_count_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH votes_per_block_root AS (
    SELECT
        slot,
        epoch,
        block_root,
        COUNT(*) as votes
    FROM `{{ index .dep "{{transformation}}" "int_attestation_attested_head" "database" }}`.`int_attestation_attested_head` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot, epoch, block_root
),

votes_per_slot AS (
  SELECT
    slot,
    epoch,
    argMax(block_root, votes) AS block_root,
    max(votes)               AS votes_actual,
    sum(votes)               AS votes_max
  FROM votes_per_block_root
  GROUP BY slot, epoch
),

blobs_per_block_root AS (
  SELECT
    slot,
    epoch,
    block_root,
    blob_count
  FROM `{{ index .dep "{{transformation}}" "int_block_blob_count_head" "database" }}`.`int_block_blob_count_head` FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),

first_seen_block AS (
  SELECT
    slot,
    epoch,
    block_root,
    min(seen_slot_start_diff) AS first_seen_slot_start_diff,
    argMin(meta_client_geo_city, seen_slot_start_diff) AS first_seen_geo_city,
    argMin(meta_client_geo_country, seen_slot_start_diff) AS first_seen_geo_country,
    argMin(meta_client_geo_country_code, seen_slot_start_diff) AS first_seen_geo_country_code,
    argMin(meta_client_geo_continent_code, seen_slot_start_diff) AS first_seen_geo_continent_code
  FROM `{{ index .dep "{{transformation}}" "int_block_first_seen_by_node" "database" }}`.`int_block_first_seen_by_node` FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  GROUP BY slot, epoch, block_root
),

first_seen_attestation AS (
  SELECT
    slot,
    epoch,
    block_root,
    min(seen_slot_start_diff) AS first_seen_slot_start_diff,
    argMin(meta_client_geo_city, seen_slot_start_diff) AS first_seen_geo_city,
    argMin(meta_client_geo_country, seen_slot_start_diff) AS first_seen_geo_country,
    argMin(meta_client_geo_country_code, seen_slot_start_diff) AS first_seen_geo_country_code,
    argMin(meta_client_geo_continent_code, seen_slot_start_diff) AS first_seen_geo_continent_code
  FROM `{{ index .dep "{{transformation}}" "int_attestation_first_seen" "database" }}`.`int_attestation_first_seen` FINAL
  WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  GROUP BY slot, epoch, block_root
),

combined_first_seen AS (
  SELECT
    slot,
    epoch,
    block_root,
    IF(
      b.first_seen_slot_start_diff IS NULL AND a.first_seen_slot_start_diff IS NULL,
      NULL,
      LEAST(
        COALESCE(b.first_seen_slot_start_diff, 999999999),
        COALESCE(a.first_seen_slot_start_diff, 999999999)
      )
    ) AS first_seen_slot_start_diff,
    IF(
      COALESCE(b.first_seen_slot_start_diff, 999999999) <= COALESCE(a.first_seen_slot_start_diff, 999999999),
      b.first_seen_geo_city,
      a.first_seen_geo_city
    ) AS first_seen_geo_city,
    IF(
      COALESCE(b.first_seen_slot_start_diff, 999999999) <= COALESCE(a.first_seen_slot_start_diff, 999999999),
      b.first_seen_geo_country,
      a.first_seen_geo_country
    ) AS first_seen_geo_country,
    IF(
      COALESCE(b.first_seen_slot_start_diff, 999999999) <= COALESCE(a.first_seen_slot_start_diff, 999999999),
      b.first_seen_geo_country_code,
      a.first_seen_geo_country_code
    ) AS first_seen_geo_country_code,
    IF(
      COALESCE(b.first_seen_slot_start_diff, 999999999) <= COALESCE(a.first_seen_slot_start_diff, 999999999),
      b.first_seen_geo_continent_code,
      a.first_seen_geo_continent_code
    ) AS first_seen_geo_continent_code
  FROM votes_per_slot
  LEFT JOIN first_seen_block b USING (slot, epoch, block_root)
  LEFT JOIN first_seen_attestation a USING (slot, epoch, block_root)
)

SELECT
  fromUnixTimestamp({{ .task.start }}) as updated_date_time,
  slot,
  epoch,
  block_root,
  blob_count,
  votes_max,
  votes_actual,
  first_seen_slot_start_diff,
  first_seen_geo_city,
  first_seen_geo_country,
  first_seen_geo_country_code,
  first_seen_geo_continent_code
FROM votes_per_slot
LEFT JOIN blobs_per_block_root USING (slot, epoch, block_root)
LEFT JOIN combined_first_seen USING (slot, epoch, block_root)
