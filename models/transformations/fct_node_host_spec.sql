---
table: fct_node_host_spec
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - observoor
  - host
dependencies:
  - "observoor.host_specs"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    wallclock_slot_start_date_time,
    meta_client_name,
    meta_network_name,
    CASE
        WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870'
        ELSE ''
    END AS node_class,
    host_id,
    kernel_release,
    os_name,
    architecture,
    cpu_model,
    cpu_vendor,
    cpu_online_cores,
    cpu_logical_cores,
    cpu_physical_cores,
    cpu_performance_cores,
    cpu_efficiency_cores,
    cpu_unknown_type_cores,
    cpu_core_types,
    cpu_core_type_labels,
    cpu_max_freq_khz,
    cpu_base_freq_khz,
    memory_total_bytes,
    memory_type,
    memory_speed_mts,
    memory_dimm_count,
    memory_dimm_sizes_bytes,
    memory_dimm_types,
    memory_dimm_speeds_mts,
    disk_count,
    disk_total_bytes,
    disk_names,
    disk_models,
    disk_sizes_bytes,
    disk_rotational
FROM {{ index .dep "observoor" "host_specs" "helpers" "from" }} FINAL
WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
