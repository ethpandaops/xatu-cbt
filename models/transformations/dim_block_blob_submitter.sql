---
table: dim_block_blob_submitter
type: scheduled
schedule: "@every 5s"
tags:
  - blob_submitter
  - transaction
dependencies:
  - "{{external}}.blob_submitter"
  - "{{external}}.execution_transaction"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- latest nonzero block number
    (SELECT max(block_number) AS block_number FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL) AS latest_block_number,

    -- earliest block number
    (SELECT min(block_number)
     FROM {{ index .dep "{{external}}" "execution_transaction" "helpers" "from" }} FINAL
     WHERE meta_network_name = '{{ .env.NETWORK }}'
    ) AS earliest_block_number,

    -- choose latest if > 0, otherwise earliest
    (SELECT
        if(latest_block_number > 0, latest_block_number, earliest_block_number)
    ) AS reference_block_number

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    et.block_number,
    et.transaction_hash,
    et.address,
    ifNull(nullIf(bs.name, ''), 'Unknown') AS name
FROM
(
    SELECT
        block_number,
        transaction_hash,
        from_address AS address
    FROM {{ index .dep "{{external}}" "execution_transaction" "helpers" "from" }} FINAL
    WHERE
        block_number BETWEEN reference_block_number
                         AND reference_block_number + 1000
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND transaction_type = 3
        AND success = true
) AS et
GLOBAL LEFT JOIN
(
    SELECT address, name
    FROM {{ index .dep "{{external}}" "blob_submitter" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
) AS bs
ON lower(et.address) = lower(bs.address);
