ALTER TABLE admin_cbt_incremental_local ON CLUSTER '{cluster}'
    MODIFY COMMENT '';

ALTER TABLE admin_cbt_incremental ON CLUSTER '{cluster}'
    MODIFY COMMENT '';

ALTER TABLE fct_execution_gas_limit_signalling_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly snapshots of validator gas limit signalling using rolling 7-day window';

ALTER TABLE fct_execution_gas_limit_signalling_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT '';

ALTER TABLE fct_execution_gas_limit_signalling_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily snapshots of validator gas limit signalling using rolling 7-day window';

ALTER TABLE fct_execution_gas_limit_signalling_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT '';
