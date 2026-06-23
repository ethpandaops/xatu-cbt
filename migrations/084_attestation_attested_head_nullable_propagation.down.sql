ALTER TABLE int_attestation_attested_head ON CLUSTER '{cluster}'
    MODIFY COLUMN `propagation_distance` UInt32 COMMENT 'The distance from the slot when the attestation was propagated. 0 means the attestation was propagated within the same slot as its duty was assigned, 1 means the attestation was propagated within the next slot, etc.' CODEC(DoubleDelta, ZSTD(1));

ALTER TABLE int_attestation_attested_head_local ON CLUSTER '{cluster}'
    MODIFY COLUMN `propagation_distance` UInt32 COMMENT 'The distance from the slot when the attestation was propagated. 0 means the attestation was propagated within the same slot as its duty was assigned, 1 means the attestation was propagated within the next slot, etc.' CODEC(DoubleDelta, ZSTD(1));
