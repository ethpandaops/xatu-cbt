CREATE DATABASE IF NOT EXISTS `${NETWORK_NAME}` ON CLUSTER '{cluster}';

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_beacon_committee AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_beacon_committee');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_attestation AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_attestation');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_blob_sidecar AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_blob_sidecar');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_block_gossip AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_block_gossip');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_block AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_block');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_chain_reorg AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_chain_reorg');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_contribution_and_proof AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_contribution_and_proof');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_data_column_sidecar AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_data_column_sidecar');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_finalized_checkpoint AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_finalized_checkpoint');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_head AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_head');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_events_voluntary_exit AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_events_voluntary_exit');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_proposer_duty AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_proposer_duty');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v1_validator_attestation_data AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v1_validator_attestation_data');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v2_beacon_block AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v2_beacon_block');

CREATE VIEW ${NETWORK_NAME}.beacon_api_eth_v3_validator_block AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_api_eth_v3_validator_block');

CREATE VIEW ${NETWORK_NAME}.beacon_block_classification AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'beacon_block_classification');

CREATE VIEW ${NETWORK_NAME}.block_native_mempool_transaction AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'block_native_mempool_transaction');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_blob_sidecar AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_blob_sidecar');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_block_attester_slashing AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_block_attester_slashing');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_block_bls_to_execution_change AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_block_bls_to_execution_change');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_block_deposit AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_block_deposit');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_block_execution_transaction AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_block_execution_transaction');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_block AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_block');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_block_proposer_slashing AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_block_proposer_slashing');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_block_voluntary_exit AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_block_voluntary_exit');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_block_withdrawal AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_block_withdrawal');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_committee AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_committee');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_elaborated_attestation AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_elaborated_attestation');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_proposer_duty AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_proposer_duty');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_validators AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_validators');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_validators_pubkeys AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_validators_pubkeys');

CREATE VIEW ${NETWORK_NAME}.canonical_beacon_validators_withdrawal_credentials AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_beacon_validators_withdrawal_credentials');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_address_appearances AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_address_appearances');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_balance_diffs AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_balance_diffs');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_balance_reads AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_balance_reads');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_block AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_block');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_contracts AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_contracts');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_erc20_transfers AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_erc20_transfers');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_erc721_transfers AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_erc721_transfers');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_four_byte_counts AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_four_byte_counts');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_logs AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_logs');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_native_transfers AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_native_transfers');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_nonce_diffs AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_nonce_diffs');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_nonce_reads AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_nonce_reads');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_storage_diffs AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_storage_diffs');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_storage_reads AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_storage_reads');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_traces AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_traces');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_transaction AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_transaction');

CREATE VIEW ${NETWORK_NAME}.canonical_execution_transaction_structlog AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'canonical_execution_transaction_structlog');

CREATE VIEW ${NETWORK_NAME}.ethseer_validator_entity AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'ethseer_validator_entity');

CREATE VIEW ${NETWORK_NAME}.libp2p_add_peer AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_add_peer');

CREATE VIEW ${NETWORK_NAME}.libp2p_connected AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_connected');

CREATE VIEW ${NETWORK_NAME}.libp2p_deliver_message AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_deliver_message');

CREATE VIEW ${NETWORK_NAME}.libp2p_disconnected AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_disconnected');

CREATE VIEW ${NETWORK_NAME}.libp2p_drop_rpc AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_drop_rpc');

CREATE VIEW ${NETWORK_NAME}.libp2p_duplicate_message AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_duplicate_message');

CREATE VIEW ${NETWORK_NAME}.libp2p_gossipsub_aggregate_and_proof AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_gossipsub_aggregate_and_proof');

CREATE VIEW ${NETWORK_NAME}.libp2p_gossipsub_beacon_attestation AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_gossipsub_beacon_attestation');

CREATE VIEW ${NETWORK_NAME}.libp2p_gossipsub_beacon_block AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_gossipsub_beacon_block');

CREATE VIEW ${NETWORK_NAME}.libp2p_gossipsub_blob_sidecar AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_gossipsub_blob_sidecar');

CREATE VIEW ${NETWORK_NAME}.libp2p_gossipsub_data_column_sidecar AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_gossipsub_data_column_sidecar');

CREATE VIEW ${NETWORK_NAME}.libp2p_graft AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_graft');

CREATE VIEW ${NETWORK_NAME}.libp2p_handle_metadata AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_handle_metadata');

CREATE VIEW ${NETWORK_NAME}.libp2p_handle_status AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_handle_status');

CREATE VIEW ${NETWORK_NAME}.libp2p_join AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_join');

CREATE VIEW ${NETWORK_NAME}.libp2p_leave AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_leave');

CREATE VIEW ${NETWORK_NAME}.libp2p_peer AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_peer');

CREATE VIEW ${NETWORK_NAME}.libp2p_prune AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_prune');

CREATE VIEW ${NETWORK_NAME}.libp2p_publish_message AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_publish_message');

CREATE VIEW ${NETWORK_NAME}.libp2p_recv_rpc AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_recv_rpc');

CREATE VIEW ${NETWORK_NAME}.libp2p_reject_message AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_reject_message');

CREATE VIEW ${NETWORK_NAME}.libp2p_remove_peer AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_remove_peer');

CREATE VIEW ${NETWORK_NAME}.libp2p_rpc_data_column_custody_probe AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_rpc_data_column_custody_probe');

CREATE VIEW ${NETWORK_NAME}.libp2p_rpc_meta_control_graft AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_rpc_meta_control_graft');

CREATE VIEW ${NETWORK_NAME}.libp2p_rpc_meta_control_idontwant AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_rpc_meta_control_idontwant');

CREATE VIEW ${NETWORK_NAME}.libp2p_rpc_meta_control_ihave AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_rpc_meta_control_ihave');

CREATE VIEW ${NETWORK_NAME}.libp2p_rpc_meta_control_iwant AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_rpc_meta_control_iwant');

CREATE VIEW ${NETWORK_NAME}.libp2p_rpc_meta_control_prune AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_rpc_meta_control_prune');

CREATE VIEW ${NETWORK_NAME}.libp2p_rpc_meta_message AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_rpc_meta_message');

CREATE VIEW ${NETWORK_NAME}.libp2p_rpc_meta_subscription AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_rpc_meta_subscription');

CREATE VIEW ${NETWORK_NAME}.libp2p_send_rpc AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_send_rpc');

CREATE VIEW ${NETWORK_NAME}.libp2p_synthetic_heartbeat AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'libp2p_synthetic_heartbeat');

CREATE VIEW ${NETWORK_NAME}.mempool_dumpster_transaction AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'mempool_dumpster_transaction');

CREATE VIEW ${NETWORK_NAME}.mempool_transaction AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'mempool_transaction');

CREATE VIEW ${NETWORK_NAME}.mev_relay_bid_trace AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'mev_relay_bid_trace');

CREATE VIEW ${NETWORK_NAME}.mev_relay_proposer_payload_delivered AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'mev_relay_proposer_payload_delivered');

CREATE VIEW ${NETWORK_NAME}.mev_relay_validator_registration AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'mev_relay_validator_registration');

CREATE VIEW ${NETWORK_NAME}.node_record_consensus AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'node_record_consensus');

CREATE VIEW ${NETWORK_NAME}.node_record_execution AS
SELECT *
FROM cluster('${EXTERNAL_MODEL_CLUSTER}', '${NETWORK_NAME}', 'node_record_execution');
