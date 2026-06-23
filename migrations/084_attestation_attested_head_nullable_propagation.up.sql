-- Canonical-sourced attesters have no gossip-propagation timing, so
-- int_attestation_attested_head.propagation_distance must allow NULL for them
-- (matching fct_attestation_correctness_by_validator_head, which already
-- declares this column Nullable(UInt32)).
ALTER TABLE int_attestation_attested_head_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `propagation_distance` Nullable(UInt32) COMMENT 'The distance from the slot when the attestation was propagated. 0 means the attestation was propagated within the same slot as its duty was assigned, 1 means the attestation was propagated within the next slot, etc. NULL for canonical-sourced attesters, which carry no gossip-propagation timing.' CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE int_attestation_attested_head ON CLUSTER '{cluster}'
    MODIFY COLUMN `propagation_distance` Nullable(UInt32) COMMENT 'The distance from the slot when the attestation was propagated. 0 means the attestation was propagated within the same slot as its duty was assigned, 1 means the attestation was propagated within the next slot, etc. NULL for canonical-sourced attesters, which carry no gossip-propagation timing.' CODEC(DoubleDelta, ZSTD(1));
