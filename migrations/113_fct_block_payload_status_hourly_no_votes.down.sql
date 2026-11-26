ALTER TABLE fct_block_payload_status_hourly_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `status` 'PTC verdict bucket: delivered or absent';

ALTER TABLE fct_block_payload_status_hourly ON CLUSTER '{cluster}'
    COMMENT COLUMN `status` 'PTC verdict bucket: delivered or absent';

ALTER TABLE fct_block_payload_status_hourly_local ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gloas (ePBS) hourly payload delivery outcomes judged by the PTC, for delivery-rate charts';

ALTER TABLE fct_block_payload_status_hourly ON CLUSTER '{cluster}'
    MODIFY COMMENT 'Gloas (ePBS) hourly payload delivery outcomes judged by the PTC, for delivery-rate charts';
