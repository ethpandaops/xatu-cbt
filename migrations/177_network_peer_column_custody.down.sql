DROP TABLE IF EXISTS fct_peer_custody_coverage_daily ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_peer_custody_coverage_daily_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_peer_custody_by_column_daily ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS fct_peer_custody_by_column_daily_local ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS dim_peer_node ON CLUSTER '{cluster}' SYNC;
DROP TABLE IF EXISTS dim_peer_node_local ON CLUSTER '{cluster}' SYNC;
