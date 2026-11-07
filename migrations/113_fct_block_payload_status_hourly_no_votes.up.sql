-- fct_block_payload_status_hourly now counts canonical blocks only, judges them
-- with the fork-choice PTC quorum (more than PTC_SIZE // 2 = 256 present
-- votes) and adds a no_votes bucket for canonical blocks whose PTC votes never
-- reached the chain. Update the status comment on both tables.

ALTER TABLE fct_block_payload_status_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `status` 'PTC verdict bucket for canonical blocks: delivered (more than 256 of 512 PTC members voted the payload present), absent, or no_votes (the PTC votes were never included on chain)';

ALTER TABLE fct_block_payload_status_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `status` 'PTC verdict bucket for canonical blocks: delivered (more than 256 of 512 PTC members voted the payload present), absent, or no_votes (the PTC votes were never included on chain)';

ALTER TABLE fct_block_payload_status_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gloas (ePBS) hourly payload delivery outcomes of canonical blocks judged by the PTC quorum, for delivery-rate charts';

ALTER TABLE fct_block_payload_status_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gloas (ePBS) hourly payload delivery outcomes of canonical blocks judged by the PTC quorum, for delivery-rate charts';
