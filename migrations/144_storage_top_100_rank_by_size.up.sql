-- fct_storage_slot_top_100_by_bytes ranks contracts by storage size (active_slots * 65 + effective_bytes),
-- the size Lab shows, instead of effective_bytes alone.
--
-- Comments live on both the _local and the Distributed table, so both are updated.

ALTER TABLE fct_storage_slot_top_100_by_bytes_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Top 100 contracts by storage size (active_slots * 65 + effective_bytes) with expiry policies applied';

ALTER TABLE fct_storage_slot_top_100_by_bytes_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `rank` 'Rank by storage size, active_slots * 65 + effective_bytes (1=highest), based on raw state';

ALTER TABLE fct_storage_slot_top_100_by_bytes ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Top 100 contracts by storage size (active_slots * 65 + effective_bytes) with expiry policies applied';

ALTER TABLE fct_storage_slot_top_100_by_bytes ON CLUSTER '{cluster}'
    COMMENT COLUMN `rank` 'Rank by storage size, active_slots * 65 + effective_bytes (1=highest), based on raw state';
