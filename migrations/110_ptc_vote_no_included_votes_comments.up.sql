-- Clarify that a canonical block can carry zero PTC votes.
--
-- int_block_payload_ptc_vote_canonical now emits one row per canonical block
-- regardless of whether its PTC votes reached the chain: when slot N+1 is
-- missed the votes for slot N are never included, so the row carries zero
-- counts and empty included_in_*. The original comments only described the
-- orphaned case and now read as if those columns are always populated.
--
-- Comments live on both the _local and the Distributed table, so both are
-- updated. The Distributed table copies the column definitions at creation
-- time and does not inherit later changes to the local one.

ALTER TABLE int_block_payload_ptc_vote_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `block_version` 'The beacon block version of the containing block, empty when no votes were included';

ALTER TABLE int_block_payload_ptc_vote_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_slot` 'Slot of the canonical block that included the payload attestations, 0 when no votes were included';

ALTER TABLE int_block_payload_ptc_vote_canonical_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_block_root` 'Root of the canonical block that included the payload attestations, empty when no votes were included';

ALTER TABLE int_block_payload_ptc_vote_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `block_version` 'The beacon block version of the containing block, empty when no votes were included';

ALTER TABLE int_block_payload_ptc_vote_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_slot` 'Slot of the canonical block that included the payload attestations, 0 when no votes were included';

ALTER TABLE int_block_payload_ptc_vote_canonical ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_block_root` 'Root of the canonical block that included the payload attestations, empty when no votes were included';

ALTER TABLE fct_block_payload_ptc_vote_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `block_version` 'The beacon block version of the containing block, empty for orphaned rows and for canonical blocks whose votes were never included';

ALTER TABLE fct_block_payload_ptc_vote_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_slot` 'Slot of the canonical block that included the payload attestations, null for orphaned rows and for canonical blocks whose votes were never included';

ALTER TABLE fct_block_payload_ptc_vote_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_block_root` 'Root of the canonical block that included the payload attestations, null for orphaned rows and for canonical blocks whose votes were never included';

ALTER TABLE fct_block_payload_ptc_vote ON CLUSTER '{cluster}'
    COMMENT COLUMN `block_version` 'The beacon block version of the containing block, empty for orphaned rows and for canonical blocks whose votes were never included';

ALTER TABLE fct_block_payload_ptc_vote ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_slot` 'Slot of the canonical block that included the payload attestations, null for orphaned rows and for canonical blocks whose votes were never included';

ALTER TABLE fct_block_payload_ptc_vote ON CLUSTER '{cluster}'
    COMMENT COLUMN `included_in_block_root` 'Root of the canonical block that included the payload attestations, null for orphaned rows and for canonical blocks whose votes were never included';
