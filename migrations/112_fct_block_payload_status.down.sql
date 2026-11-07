ALTER TABLE fct_block_payload ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `payload_status`;

ALTER TABLE fct_block_payload_local ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `payload_status`;

ALTER TABLE fct_block_payload_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `transactions_count` 'Number of transactions in the revealed payload';

ALTER TABLE fct_block_payload ON CLUSTER '{cluster}'
    COMMENT COLUMN `transactions_count` 'Number of transactions in the revealed payload';
