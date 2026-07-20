-- Restore the column comments as created in 103_block_payload_ptc_vote.up.sql.

ALTER TABLE int_block_payload_ptc_vote_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `block_version` 'The beacon block version of the containing block';

ALTER TABLE int_block_payload_ptc_vote_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_slot` 'Slot of the canonical block that included the payload attestations';

ALTER TABLE int_block_payload_ptc_vote_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_block_root` 'Root of the canonical block that included the payload attestations';

ALTER TABLE int_block_payload_ptc_vote_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `block_version` 'The beacon block version of the containing block';

ALTER TABLE int_block_payload_ptc_vote_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_slot` 'Slot of the canonical block that included the payload attestations';

ALTER TABLE int_block_payload_ptc_vote_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_block_root` 'Root of the canonical block that included the payload attestations';

ALTER TABLE fct_block_payload_ptc_vote_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `block_version` 'The beacon block version of the containing block, empty for orphaned rows';

ALTER TABLE fct_block_payload_ptc_vote_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_slot` 'Slot of the canonical block that included the payload attestations, null for orphaned rows';

ALTER TABLE fct_block_payload_ptc_vote_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_block_root` 'Root of the canonical block that included the payload attestations, null for orphaned rows';

ALTER TABLE fct_block_payload_ptc_vote ON CLUSTER '{cluster}'
    COMMENT COLUMN `block_version` 'The beacon block version of the containing block, empty for orphaned rows';

ALTER TABLE fct_block_payload_ptc_vote ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_slot` 'Slot of the canonical block that included the payload attestations, null for orphaned rows';

ALTER TABLE fct_block_payload_ptc_vote ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_block_root` 'Root of the canonical block that included the payload attestations, null for orphaned rows';
