-- Restore the comments set in 053_storage_state_restructure.up.sql.

ALTER TABLE fct_storage_slot_top_100_by_bytes_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Top 100 contracts by effective storage bytes with expiry policies applied';

ALTER TABLE fct_storage_slot_top_100_by_bytes_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `rank` 'Rank by effective bytes (1=highest), based on raw state';

ALTER TABLE fct_storage_slot_top_100_by_bytes ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Top 100 contracts by effective storage bytes with expiry policies applied';

ALTER TABLE fct_storage_slot_top_100_by_bytes ON CLUSTER '{cluster}'
    COMMENT COLUMN `rank` 'Rank by effective bytes (1=highest), based on raw state';
