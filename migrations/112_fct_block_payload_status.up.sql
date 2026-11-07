-- Gloas (ePBS): a canonical beacon block does not make its payload canonical.
-- Record whether the next canonical block built on the payload (fork-choice
-- get_parent_payload_status), so transactions of payloads that never executed
-- on chain can be told apart.
--
-- Appended last so existing protobuf field numbers stay stable. Existing rows
-- default to '' until the model is re-run over their range.

ALTER TABLE fct_block_payload_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `payload_status` LowCardinality(String) COMMENT 'Whether the payload became canonical: full when the next canonical block built on it (its transactions executed on chain), empty when that block built on the payload parent instead (withheld, late or not built upon, so its transactions never executed), unknown when no canonical child was found' CODEC(ZSTD(1));

ALTER TABLE fct_block_payload ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `payload_status` LowCardinality(String) COMMENT 'Whether the payload became canonical: full when the next canonical block built on it (its transactions executed on chain), empty when that block built on the payload parent instead (withheld, late or not built upon, so its transactions never executed), unknown when no canonical child was found';

ALTER TABLE fct_block_payload_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `transactions_count` 'Number of transactions in the revealed payload, only executed on chain when payload_status is full';

ALTER TABLE fct_block_payload ON CLUSTER '{cluster}'
    COMMENT COLUMN `transactions_count` 'Number of transactions in the revealed payload, only executed on chain when payload_status is full';
