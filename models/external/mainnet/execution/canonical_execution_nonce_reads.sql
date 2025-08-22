---
database: mainnet
table: canonical_execution_nonce_reads
ttl: 5m
---
SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL;
