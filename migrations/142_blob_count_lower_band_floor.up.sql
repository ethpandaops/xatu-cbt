-- The blob count lower Bollinger band is floored at 0 because a blob count can't be negative.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE fct_blob_count_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `lower_band_blob_count` 'Lower Bollinger band (avg - 2*stddev), floored at 0 because a blob count cannot be negative';

ALTER TABLE fct_blob_count_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `lower_band_blob_count` 'Lower Bollinger band (avg - 2*stddev), floored at 0 because a blob count cannot be negative';

ALTER TABLE fct_blob_count_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `lower_band_blob_count` 'Lower Bollinger band (avg - 2*stddev), floored at 0 because a blob count cannot be negative';

ALTER TABLE fct_blob_count_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `lower_band_blob_count` 'Lower Bollinger band (avg - 2*stddev), floored at 0 because a blob count cannot be negative';
