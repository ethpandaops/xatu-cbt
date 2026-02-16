-- Add resource gas building block columns to int_transaction_call_frame_opcode_gas.
-- These columns enable downstream SQL to compute memory expansion gas and cold access gas
-- without needing per-opcode structlog data.

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas_local ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `memory_words_sum_before` UInt64 DEFAULT 0 COMMENT 'SUM(ceil(memory_bytes/32)) before each opcode executes.' CODEC(ZSTD(1)) AFTER `error_count`,
    ADD COLUMN IF NOT EXISTS `memory_words_sum_after` UInt64 DEFAULT 0 COMMENT 'SUM(ceil(memory_bytes/32)) after each opcode executes.' CODEC(ZSTD(1)) AFTER `memory_words_sum_before`,
    ADD COLUMN IF NOT EXISTS `memory_words_sq_sum_before` UInt64 DEFAULT 0 COMMENT 'SUM(words_before²).' CODEC(ZSTD(1)) AFTER `memory_words_sum_after`,
    ADD COLUMN IF NOT EXISTS `memory_words_sq_sum_after` UInt64 DEFAULT 0 COMMENT 'SUM(words_after²).' CODEC(ZSTD(1)) AFTER `memory_words_sq_sum_before`,
    ADD COLUMN IF NOT EXISTS `cold_access_count` UInt64 DEFAULT 0 COMMENT 'Number of cold storage/account accesses (EIP-2929).' CODEC(ZSTD(1)) AFTER `memory_words_sq_sum_after`;

ALTER TABLE `${NETWORK_NAME}`.int_transaction_call_frame_opcode_gas ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `memory_words_sum_before` UInt64 DEFAULT 0 COMMENT 'SUM(ceil(memory_bytes/32)) before each opcode executes.' AFTER `error_count`,
    ADD COLUMN IF NOT EXISTS `memory_words_sum_after` UInt64 DEFAULT 0 COMMENT 'SUM(ceil(memory_bytes/32)) after each opcode executes.' AFTER `memory_words_sum_before`,
    ADD COLUMN IF NOT EXISTS `memory_words_sq_sum_before` UInt64 DEFAULT 0 COMMENT 'SUM(words_before²).' AFTER `memory_words_sum_after`,
    ADD COLUMN IF NOT EXISTS `memory_words_sq_sum_after` UInt64 DEFAULT 0 COMMENT 'SUM(words_after²).' AFTER `memory_words_sq_sum_before`,
    ADD COLUMN IF NOT EXISTS `cold_access_count` UInt64 DEFAULT 0 COMMENT 'Number of cold storage/account accesses (EIP-2929).' AFTER `memory_words_sq_sum_after`;
