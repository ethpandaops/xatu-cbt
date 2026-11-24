-- Restore the comments set in 059_fct_blob_count.up.sql.

ALTER TABLE fct_blob_count_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `lower_band_blob_count` 'Lower Bollinger band (avg - 2*stddev)';

ALTER TABLE fct_blob_count_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `lower_band_blob_count` 'Lower Bollinger band (avg - 2*stddev)';

ALTER TABLE fct_blob_count_daily_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `lower_band_blob_count` 'Lower Bollinger band (avg - 2*stddev)';

ALTER TABLE fct_blob_count_daily ON CLUSTER '{cluster}'
    COMMENT COLUMN `lower_band_blob_count` 'Lower Bollinger band (avg - 2*stddev)';
