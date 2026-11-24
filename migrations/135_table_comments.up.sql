ALTER TABLE admin_cbt_incremental_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'CBT bookkeeping for incremental transformation models: one row per interval CBT has processed, with the model database and table, the interval starting position and its size. A model has processed the union of its rows. Positions are in the model interval units, such as Unix seconds or block numbers';

ALTER TABLE admin_cbt_incremental ON CLUSTER '{cluster}'
    MODIFY COMMENT 'CBT bookkeeping for incremental transformation models: one row per interval CBT has processed, with the model database and table, the interval starting position and its size. A model has processed the union of its rows. Positions are in the model interval units, such as Unix seconds or block numbers';

ALTER TABLE fct_execution_gas_limit_signalling_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly gas limit signalling by validators, from their MEV relay validator registrations. For each hour, each validator counts once at the highest gas limit it registered on that UTC day or the 7 days before, rounded up to a multiple of 1M gas. gas_limit_band_counts maps each band to its validator count. Validators that do not register with the relays xatu polls are not counted';

ALTER TABLE fct_execution_gas_limit_signalling_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Hourly gas limit signalling by validators, from their MEV relay validator registrations. For each hour, each validator counts once at the highest gas limit it registered on that UTC day or the 7 days before, rounded up to a multiple of 1M gas. gas_limit_band_counts maps each band to its validator count. Validators that do not register with the relays xatu polls are not counted';

ALTER TABLE fct_execution_gas_limit_signalling_daily_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily gas limit signalling by validators, from their MEV relay validator registrations. For each UTC day, each validator counts once at the highest gas limit it registered in the 7 days ending that day, rounded up to a multiple of 1M gas. gas_limit_band_counts maps each band to its validator count. Validators that do not register with the relays xatu polls are not counted';

ALTER TABLE fct_execution_gas_limit_signalling_daily ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Daily gas limit signalling by validators, from their MEV relay validator registrations. For each UTC day, each validator counts once at the highest gas limit it registered in the 7 days ending that day, rounded up to a multiple of 1M gas. gas_limit_band_counts maps each band to its validator count. Validators that do not register with the relays xatu polls are not counted';
