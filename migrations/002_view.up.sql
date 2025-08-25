CREATE DATABASE IF NOT EXISTS mainnet ON CLUSTER '{cluster}';

CREATE VIEW mainnet.beacon_api_eth_v1_beacon_committee ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_beacon_committee
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_attestation ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_attestation
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_blob_sidecar ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_blob_sidecar
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_block_gossip ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_block_gossip
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_block ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_block
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_chain_reorg ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_chain_reorg
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_contribution_and_proof ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_contribution_and_proof
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_data_column_sidecar ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_data_column_sidecar
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_finalized_checkpoint ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_finalized_checkpoint
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_head ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_head
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_events_voluntary_exit ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_events_voluntary_exit
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_proposer_duty ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_proposer_duty
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v1_validator_attestation_data ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v1_validator_attestation_data
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v2_beacon_block ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v2_beacon_block
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_api_eth_v3_validator_block ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_api_eth_v3_validator_block
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.beacon_block_classification ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.beacon_block_classification
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.block_native_mempool_transaction ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.block_native_mempool_transaction
WHERE
    network = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_blob_sidecar ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_blob_sidecar
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_block_attester_slashing ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_block_attester_slashing
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_block_bls_to_execution_change ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_block_bls_to_execution_change
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_block_deposit ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_block_deposit
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_block_execution_transaction ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_block_execution_transaction
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_block ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_block
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_block_proposer_slashing ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_block_proposer_slashing
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_block_voluntary_exit ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_block_voluntary_exit
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_block_withdrawal ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_block_withdrawal
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_committee ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_committee
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_elaborated_attestation ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_elaborated_attestation
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_proposer_duty ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_proposer_duty
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_validators ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_validators
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_validators_pubkeys ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_validators_pubkeys
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_beacon_validators_withdrawal_credentials ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_beacon_validators_withdrawal_credentials
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_address_appearances ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_address_appearances
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_balance_diffs ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_balance_diffs
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_balance_reads ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_balance_reads
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_block ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_block
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_contracts ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_contracts
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_erc20_transfers ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_erc20_transfers
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_erc721_transfers ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_erc721_transfers
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_four_byte_counts ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_four_byte_counts
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_logs ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_logs
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_native_transfers ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_native_transfers
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_nonce_diffs ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_nonce_diffs
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_nonce_reads ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_nonce_reads
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_storage_diffs ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_storage_diffs
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_storage_reads ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_storage_reads
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_traces ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_traces
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_transaction ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_transaction
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.canonical_execution_transaction_structlog ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.canonical_execution_transaction_structlog
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_add_peer ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_add_peer
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_connected ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_connected
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_deliver_message ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_deliver_message
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_disconnected ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_disconnected
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_drop_rpc ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_drop_rpc
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_duplicate_message ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_duplicate_message
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_gossipsub_aggregate_and_proof ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_gossipsub_aggregate_and_proof
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_gossipsub_beacon_attestation ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_gossipsub_beacon_attestation
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_gossipsub_beacon_block ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_gossipsub_beacon_block
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_gossipsub_blob_sidecar ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_gossipsub_blob_sidecar
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_gossipsub_data_column_sidecar ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_gossipsub_data_column_sidecar
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_graft ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_graft
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_handle_metadata ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_handle_metadata
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_handle_status ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_handle_status
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_join ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_join
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_leave ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_leave
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_peer ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_peer
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_prune ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_prune
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_publish_message ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_publish_message
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_recv_rpc ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_recv_rpc
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_reject_message ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_reject_message
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_remove_peer ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_remove_peer
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_rpc_meta_control_graft ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_rpc_meta_control_graft
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_rpc_meta_control_idontwant ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_rpc_meta_control_idontwant
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_rpc_meta_control_ihave ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_rpc_meta_control_ihave
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_rpc_meta_control_iwant ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_rpc_meta_control_iwant
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_rpc_meta_control_prune ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_rpc_meta_control_prune
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_rpc_meta_message ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_rpc_meta_message
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_rpc_meta_subscription ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_rpc_meta_subscription
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_send_rpc ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_send_rpc
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.libp2p_synthetic_heartbeat ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.libp2p_synthetic_heartbeat
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.mempool_dumpster_transaction ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.mempool_dumpster_transaction
WHERE
    chain_id = 1;

CREATE VIEW mainnet.mempool_transaction ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.mempool_transaction
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.mev_relay_bid_trace ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.mev_relay_bid_trace
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.mev_relay_proposer_payload_delivered ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.mev_relay_proposer_payload_delivered
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.mev_relay_validator_registration ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.mev_relay_validator_registration
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.node_record_consensus ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.node_record_consensus
WHERE
    meta_network_name = 'mainnet';

CREATE VIEW mainnet.node_record_execution ON CLUSTER '{cluster}' AS
SELECT
    *
FROM
    default.node_record_execution
WHERE
    meta_network_name = 'mainnet';
