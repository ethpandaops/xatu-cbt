DROP TABLE IF EXISTS dim_client_release ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS dim_client_release_local ON CLUSTER '{cluster}';

ALTER TABLE fct_block_graffiti_local ON CLUSTER '{cluster}'
    COMMENT COLUMN `graffiti_format` 'Which graffiti format identified the clients: codes_full, codes_long, codes_short or codes_min (client version codes with 8, 4, 2 or no commit characters), version_string (a client default such as Lighthouse/v8.2.3-6db6ae9), none (unrecognised text) or empty';

ALTER TABLE fct_block_graffiti ON CLUSTER '{cluster}'
    COMMENT COLUMN `graffiti_format` 'Which graffiti format identified the clients: codes_full, codes_long, codes_short or codes_min (client version codes with 8, 4, 2 or no commit characters), version_string (a client default such as Lighthouse/v8.2.3-6db6ae9), none (unrecognised text) or empty';
