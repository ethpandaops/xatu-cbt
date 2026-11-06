-- Restore the comments from 025 and 138, and drop the gossip table.

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index, from custody probe results and gossipsub data column sidecar sightings. A gossipsub sighting always counts as a success. A custody probe only counts if it was recorded before the slot was processed, and probes usually arrive hours later, so almost all rows are gossipsub sightings';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index, from custody probe results and gossipsub data column sidecar sightings. A gossipsub sighting always counts as a success. A custody probe only counts if it was recorded before the slot was processed, and probes usually arrive hours later, so almost all rows are gossipsub sightings';

ALTER TABLE fct_data_column_availability_by_slot_blob_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index, repeated once per blob index of the slot. Probes and sightings are per column, so every blob of a slot carries the same counts. Sources as fct_data_column_availability_by_slot: almost all rows are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_by_slot_blob ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by slot and column index, repeated once per blob index of the slot. Probes and sightings are per column, so every blob of a slot carries the same counts. Sources as fct_data_column_availability_by_slot: almost all rows are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_by_epoch_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by epoch and column index, rolled up from fct_data_column_availability_by_slot: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_by_epoch ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by epoch and column index, rolled up from fct_data_column_availability_by_slot: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by hour and column index, rolled up from fct_data_column_availability_by_epoch: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by hour and column index, rolled up from fct_data_column_availability_by_epoch: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by UTC day and column index, rolled up from fct_data_column_availability_hourly: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Data column availability by UTC day and column index, rolled up from fct_data_column_availability_hourly: almost all observations are gossipsub sightings, which always count as a success';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_slot` 'The wallclock slot when the request was sent';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_slot` 'The wallclock slot when the request was sent';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_slot_start_date_time` 'The start time for the slot when the request was sent';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_slot_start_date_time` 'The start time for the slot when the request was sent';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_epoch` 'The wallclock epoch when the request was sent';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_epoch` 'The wallclock epoch when the request was sent';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_epoch_start_date_time` 'The start time for the wallclock epoch when the request was sent';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_epoch_start_date_time` 'The start time for the wallclock epoch when the request was sent';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `beacon_block_root` 'Beacon block root for this slot (from earliest observation, handles reorgs, NULL if unavailable)';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `beacon_block_root` 'Beacon block root for this slot (from earliest observation, handles reorgs, NULL if unavailable)';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `beacon_block_root_variants` 'Number of unique block roots observed (>1 indicates reorg/fork)';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `beacon_block_root_variants` 'Number of unique block roots observed (>1 indicates reorg/fork)';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'Number of blobs in the slot (max column_rows_count)';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'Number of blobs in the slot (max column_rows_count)';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `success_count` 'Count of successful probes';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `success_count` 'Count of successful probes';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `failure_count` 'Count of failed probes (result = failure)';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `failure_count` 'Count of failed probes (result = failure)';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `missing_count` 'Count of missing probes (result = missing)';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `missing_count` 'Count of missing probes (result = missing)';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `probe_count` 'Total count of probes';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `probe_count` 'Total count of probes';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `availability_pct` 'Availability percentage (success / total * 100) rounded to 2 decimal places';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `availability_pct` 'Availability percentage (success / total * 100) rounded to 2 decimal places';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_peer_count` 'Count of unique peers probed';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_peer_count` 'Count of unique peers probed';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_client_count` 'Count of unique client names';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_client_count` 'Count of unique client names';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_implementation_count` 'Count of unique client implementations';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_implementation_count` 'Count of unique client implementations';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `custody_probe_count` 'Count of observations from active RPC custody probes';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `custody_probe_count` 'Count of observations from active RPC custody probes';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `gossipsub_count` 'Count of observations from passive gossipsub propagation';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `gossipsub_count` 'Count of observations from passive gossipsub propagation';

DROP TABLE IF EXISTS int_data_column_gossip_by_slot ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS int_data_column_gossip_by_slot_local ON CLUSTER '{cluster}' SYNC;
