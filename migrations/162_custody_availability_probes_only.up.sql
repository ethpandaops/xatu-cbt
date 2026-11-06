-- The custody availability tables count custody probes only and recount the last 19 days, so probes that arrive
-- after a slot land in its row. Gossipsub sightings move to int_data_column_gossip_by_slot, and probes are also
-- counted by the age of the slot they tested.

CREATE TABLE int_data_column_gossip_by_slot_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `slot` UInt32 COMMENT 'The slot number' CODEC(DoubleDelta, ZSTD(1)),
    `slot_start_date_time` DateTime COMMENT 'The wall clock time when the slot started' CODEC(DoubleDelta, ZSTD(1)),
    `epoch` UInt32 COMMENT 'The epoch number containing the slot' CODEC(DoubleDelta, ZSTD(1)),
    `epoch_start_date_time` DateTime COMMENT 'The wall clock time when the epoch started' CODEC(DoubleDelta, ZSTD(1)),
    `column_index` UInt64 COMMENT 'Data column index (0-127)' CODEC(ZSTD(1)),
    `beacon_block_root` String COMMENT 'Beacon block root of the earliest gossipsub sighting of the column' CODEC(ZSTD(1)),
    `beacon_block_root_variants` UInt8 COMMENT 'Number of block roots in the sightings (more than 1 means a fork)' CODEC(ZSTD(1)),
    `blob_count` UInt16 COMMENT 'Number of blobs in the slot, the most KZG commitments a sighting carried' CODEC(ZSTD(1)),
    `gossipsub_count` UInt32 COMMENT 'Number of gossipsub sightings of the column whose block root a beacon node accepted' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(slot_start_date_time)
ORDER BY
    (`slot_start_date_time`, `column_index`)
COMMENT 'Gossipsub data column sidecar sightings per slot and column, from libp2p sentries, counting only sidecars whose block root a beacon node accepted';

CREATE TABLE int_data_column_gossip_by_slot ON CLUSTER '{cluster}' AS int_data_column_gossip_by_slot_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    int_data_column_gossip_by_slot_local,
    cityHash64(`slot_start_date_time`, `column_index`)
);

CREATE TABLE fct_custody_probe_by_slot_age_hourly_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `hour_start_date_time` DateTime COMMENT 'The hour the probes ran' CODEC(DoubleDelta, ZSTD(1)),
    `slot_age_bucket` LowCardinality(String) COMMENT 'Age of the probed slot when the probe ran: 0-1d, 1-3d, 3-7d, 7-14d, 14-18d, or 18d+ (past the 4096-epoch retention of about 18.2 days)',
    `slot_age_min_days` UInt8 COMMENT 'Start of the slot age bucket in days, for ordering the buckets' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client of the probed P2P peer, from its agent (erigon is caplin, unknown when the agent is empty)',
    `probe_count` UInt64 COMMENT 'Number of column probes, one per column a probe asked for' CODEC(ZSTD(1)),
    `success_count` UInt64 COMMENT 'Column probes the peer answered with the column' CODEC(ZSTD(1)),
    `missing_count` UInt64 COMMENT 'Column probes the peer answered without the column' CODEC(ZSTD(1)),
    `failure_count` UInt64 COMMENT 'Column probes that failed' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfMonth(hour_start_date_time)
ORDER BY
    (`hour_start_date_time`, `slot_age_bucket`, `client`)
COMMENT 'Data column custody probes per hour the probes ran, age of the probed slot and P2P peer client. Late age buckets show peers that stop custodying columns before retention ends';

CREATE TABLE fct_custody_probe_by_slot_age_hourly ON CLUSTER '{cluster}' AS fct_custody_probe_by_slot_age_hourly_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_custody_probe_by_slot_age_hourly_local,
    cityHash64(`hour_start_date_time`)
);

CREATE TABLE fct_custody_probe_by_slot_age_daily_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime COMMENT 'Timestamp when the record was last updated' CODEC(DoubleDelta, ZSTD(1)),
    `day_start_date` Date COMMENT 'The UTC day the probes ran' CODEC(DoubleDelta, ZSTD(1)),
    `slot_age_bucket` LowCardinality(String) COMMENT 'Age of the probed slot when the probe ran: 0-1d, 1-3d, 3-7d, 7-14d, 14-18d, or 18d+ (past the 4096-epoch retention of about 18.2 days)',
    `slot_age_min_days` UInt8 COMMENT 'Start of the slot age bucket in days, for ordering the buckets' CODEC(ZSTD(1)),
    `client` LowCardinality(String) COMMENT 'Consensus client of the probed P2P peer, from its agent (erigon is caplin, unknown when the agent is empty)',
    `probe_count` UInt64 COMMENT 'Number of column probes, one per column a probe asked for' CODEC(ZSTD(1)),
    `success_count` UInt64 COMMENT 'Column probes the peer answered with the column' CODEC(ZSTD(1)),
    `missing_count` UInt64 COMMENT 'Column probes the peer answered without the column' CODEC(ZSTD(1)),
    `failure_count` UInt64 COMMENT 'Column probes that failed' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    `updated_date_time`
) PARTITION BY toStartOfYear(day_start_date)
ORDER BY
    (`day_start_date`, `slot_age_bucket`, `client`)
COMMENT 'Data column custody probes per UTC day the probes ran, age of the probed slot and P2P peer client. Late age buckets show peers that stop custodying columns before retention ends';

CREATE TABLE fct_custody_probe_by_slot_age_daily ON CLUSTER '{cluster}' AS fct_custody_probe_by_slot_age_daily_local ENGINE = Distributed(
    '{cluster}',
    currentDatabase(),
    fct_custody_probe_by_slot_age_daily_local,
    cityHash64(`day_start_date`)
);

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by slot and column index. Only custody probes count towards success, failure, missing and availability. Gossipsub sightings are counted separately in gossipsub_count. Probes keep arriving for about 18 days after a slot, so the last 19 days are recounted every hour';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by slot and column index. Only custody probes count towards success, failure, missing and availability. Gossipsub sightings are counted separately in gossipsub_count. Probes keep arriving for about 18 days after a slot, so the last 19 days are recounted every hour';

ALTER TABLE fct_data_column_availability_by_slot_blob_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by slot and column index, repeated once per blob index of the slot. Probes are per column, so every blob of a slot carries the same counts. Recounted every 6 hours over the last 19 days';

ALTER TABLE fct_data_column_availability_by_slot_blob ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by slot and column index, repeated once per blob index of the slot. Probes are per column, so every blob of a slot carries the same counts. Recounted every 6 hours over the last 19 days';

ALTER TABLE fct_data_column_availability_by_epoch_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by epoch and column index, rolled up from fct_data_column_availability_by_slot. Only custody probes count. The last 19 days are recounted every hour';

ALTER TABLE fct_data_column_availability_by_epoch ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by epoch and column index, rolled up from fct_data_column_availability_by_slot. Only custody probes count. The last 19 days are recounted every hour';

ALTER TABLE fct_data_column_availability_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by hour and column index, rolled up from fct_data_column_availability_by_epoch. Only custody probes count. The last 19 days are recounted every hour';

ALTER TABLE fct_data_column_availability_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by hour and column index, rolled up from fct_data_column_availability_by_epoch. Only custody probes count. The last 19 days are recounted every hour';

ALTER TABLE fct_data_column_availability_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by UTC day and column index, rolled up from fct_data_column_availability_hourly. Only custody probes count. The last 19 days are recounted every hour';

ALTER TABLE fct_data_column_availability_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Custody probe results by UTC day and column index, rolled up from fct_data_column_availability_hourly. Only custody probes count. The last 19 days are recounted every hour';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_slot` 'The slot the probes tested, the same as slot';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_slot` 'The slot the probes tested, the same as slot';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_slot_start_date_time` 'The start time of the slot the probes tested, the same as slot_start_date_time';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_slot_start_date_time` 'The start time of the slot the probes tested, the same as slot_start_date_time';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_epoch` 'The epoch of the slot the probes tested, the same as epoch';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_epoch` 'The epoch of the slot the probes tested, the same as epoch';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_epoch_start_date_time` 'The start time of the epoch the probes tested, the same as epoch_start_date_time';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `wallclock_request_epoch_start_date_time` 'The start time of the epoch the probes tested, the same as epoch_start_date_time';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `beacon_block_root` 'Beacon block root from the earliest gossipsub sighting of the column, NULL when it was not gossiped';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `beacon_block_root` 'Beacon block root from the earliest gossipsub sighting of the column, NULL when it was not gossiped';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `beacon_block_root_variants` 'Number of block roots in the gossipsub sightings (more than 1 means a fork), 0 when not gossiped';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `beacon_block_root_variants` 'Number of block roots in the gossipsub sightings (more than 1 means a fork), 0 when not gossiped';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'Number of blobs in the slot, from the probes and the gossipsub sightings';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `blob_count` 'Number of blobs in the slot, from the probes and the gossipsub sightings';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `success_count` 'Number of custody probes that returned the column';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `success_count` 'Number of custody probes that returned the column';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `failure_count` 'Number of custody probes that failed (result = failure)';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `failure_count` 'Number of custody probes that failed (result = failure)';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `missing_count` 'Number of custody probes the peer answered without the column (result = missing)';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `missing_count` 'Number of custody probes the peer answered without the column (result = missing)';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `probe_count` 'Number of custody probes of the column for this slot';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `probe_count` 'Number of custody probes of the column for this slot';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `availability_pct` 'Share of custody probes that returned the column (success / probe_count * 100), rounded to 2 decimal places';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `availability_pct` 'Share of custody probes that returned the column (success / probe_count * 100), rounded to 2 decimal places';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_peer_count` 'Number of distinct P2P peers probed';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_peer_count` 'Number of distinct P2P peers probed';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_client_count` 'Number of distinct Xatu nodes that sent the probes';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_client_count` 'Number of distinct Xatu nodes that sent the probes';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_implementation_count` 'Number of distinct client implementations that sent the probes';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `unique_implementation_count` 'Number of distinct client implementations that sent the probes';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `custody_probe_count` 'Number of custody probes, the same as probe_count';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `custody_probe_count` 'Number of custody probes, the same as probe_count';

ALTER TABLE fct_data_column_availability_by_slot_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `gossipsub_count` 'Number of gossipsub sightings of the column, which do not count towards availability';

ALTER TABLE fct_data_column_availability_by_slot ON CLUSTER '{cluster}'
    COMMENT COLUMN `gossipsub_count` 'Number of gossipsub sightings of the column, which do not count towards availability';
