---
database: mainnet
table: canonical_execution_balance_reads
ttl: 5m
---
SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM mainnet.canonical_execution_balance_reads FINAL;
