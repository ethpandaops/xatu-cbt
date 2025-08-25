-- Rollback for beacon_api_eth_v1_beacon_committee

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_beacon_committee ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_attestation

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_attestation ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_blob_sidecar

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_blob_sidecar ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_block_gossip

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_block_gossip ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_block

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_block ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_chain_reorg

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_chain_reorg ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_contribution_and_proof

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_contribution_and_proof ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_data_column_sidecar

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_data_column_sidecar ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_finalized_checkpoint

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_finalized_checkpoint ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_head

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_head ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_events_voluntary_exit

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_events_voluntary_exit ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_proposer_duty

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_proposer_duty ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v1_validator_attestation_data

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v1_validator_attestation_data ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v2_beacon_block

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v2_beacon_block ON CLUSTER '{cluster}';

-- Rollback for beacon_api_eth_v3_validator_block

DROP VIEW IF EXISTS mainnet.beacon_api_eth_v3_validator_block ON CLUSTER '{cluster}';

-- Rollback for beacon_block_classification

DROP VIEW IF EXISTS mainnet.beacon_block_classification ON CLUSTER '{cluster}';

-- Rollback for block_native_mempool_transaction

DROP VIEW IF EXISTS mainnet.block_native_mempool_transaction ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_blob_sidecar

DROP VIEW IF EXISTS mainnet.canonical_beacon_blob_sidecar ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_block_attester_slashing

DROP VIEW IF EXISTS mainnet.canonical_beacon_block_attester_slashing ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_block_bls_to_execution_change

DROP VIEW IF EXISTS mainnet.canonical_beacon_block_bls_to_execution_change ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_block_deposit

DROP VIEW IF EXISTS mainnet.canonical_beacon_block_deposit ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_block_execution_transaction

DROP VIEW IF EXISTS mainnet.canonical_beacon_block_execution_transaction ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_block

DROP VIEW IF EXISTS mainnet.canonical_beacon_block ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_block_proposer_slashing

DROP VIEW IF EXISTS mainnet.canonical_beacon_block_proposer_slashing ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_block_voluntary_exit

DROP VIEW IF EXISTS mainnet.canonical_beacon_block_voluntary_exit ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_block_withdrawal

DROP VIEW IF EXISTS mainnet.canonical_beacon_block_withdrawal ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_committee

DROP VIEW IF EXISTS mainnet.canonical_beacon_committee ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_elaborated_attestation

DROP VIEW IF EXISTS mainnet.canonical_beacon_elaborated_attestation ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_proposer_duty

DROP VIEW IF EXISTS mainnet.canonical_beacon_proposer_duty ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_validators

DROP VIEW IF EXISTS mainnet.canonical_beacon_validators ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_validators_pubkeys

DROP VIEW IF EXISTS mainnet.canonical_beacon_validators_pubkeys ON CLUSTER '{cluster}';

-- Rollback for canonical_beacon_validators_withdrawal_credentials

DROP VIEW IF EXISTS mainnet.canonical_beacon_validators_withdrawal_credentials ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_address_appearances

DROP VIEW IF EXISTS mainnet.canonical_execution_address_appearances ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_balance_diffs

DROP VIEW IF EXISTS mainnet.canonical_execution_balance_diffs ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_balance_reads

DROP VIEW IF EXISTS mainnet.canonical_execution_balance_reads ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_block

DROP VIEW IF EXISTS mainnet.canonical_execution_block ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_contracts

DROP VIEW IF EXISTS mainnet.canonical_execution_contracts ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_erc20_transfers

DROP VIEW IF EXISTS mainnet.canonical_execution_erc20_transfers ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_erc721_transfers

DROP VIEW IF EXISTS mainnet.canonical_execution_erc721_transfers ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_four_byte_counts

DROP VIEW IF EXISTS mainnet.canonical_execution_four_byte_counts ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_logs

DROP VIEW IF EXISTS mainnet.canonical_execution_logs ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_native_transfers

DROP VIEW IF EXISTS mainnet.canonical_execution_native_transfers ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_nonce_diffs

DROP VIEW IF EXISTS mainnet.canonical_execution_nonce_diffs ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_nonce_reads

DROP VIEW IF EXISTS mainnet.canonical_execution_nonce_reads ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_storage_diffs

DROP VIEW IF EXISTS mainnet.canonical_execution_storage_diffs ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_storage_reads

DROP VIEW IF EXISTS mainnet.canonical_execution_storage_reads ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_traces

DROP VIEW IF EXISTS mainnet.canonical_execution_traces ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_transaction

DROP VIEW IF EXISTS mainnet.canonical_execution_transaction ON CLUSTER '{cluster}';

-- Rollback for canonical_execution_transaction_structlog

DROP VIEW IF EXISTS mainnet.canonical_execution_transaction_structlog ON CLUSTER '{cluster}';

-- Rollback for libp2p_add_peer

DROP VIEW IF EXISTS mainnet.libp2p_add_peer ON CLUSTER '{cluster}';

-- Rollback for libp2p_connected

DROP VIEW IF EXISTS mainnet.libp2p_connected ON CLUSTER '{cluster}';

-- Rollback for libp2p_deliver_message

DROP VIEW IF EXISTS mainnet.libp2p_deliver_message ON CLUSTER '{cluster}';

-- Rollback for libp2p_disconnected

DROP VIEW IF EXISTS mainnet.libp2p_disconnected ON CLUSTER '{cluster}';

-- Rollback for libp2p_drop_rpc

DROP VIEW IF EXISTS mainnet.libp2p_drop_rpc ON CLUSTER '{cluster}';

-- Rollback for libp2p_duplicate_message

DROP VIEW IF EXISTS mainnet.libp2p_duplicate_message ON CLUSTER '{cluster}';

-- Rollback for libp2p_gossipsub_aggregate_and_proof

DROP VIEW IF EXISTS mainnet.libp2p_gossipsub_aggregate_and_proof ON CLUSTER '{cluster}';

-- Rollback for libp2p_gossipsub_beacon_attestation

DROP VIEW IF EXISTS mainnet.libp2p_gossipsub_beacon_attestation ON CLUSTER '{cluster}';

-- Rollback for libp2p_gossipsub_beacon_block

DROP VIEW IF EXISTS mainnet.libp2p_gossipsub_beacon_block ON CLUSTER '{cluster}';

-- Rollback for libp2p_gossipsub_blob_sidecar

DROP VIEW IF EXISTS mainnet.libp2p_gossipsub_blob_sidecar ON CLUSTER '{cluster}';

-- Rollback for libp2p_gossipsub_data_column_sidecar

DROP VIEW IF EXISTS mainnet.libp2p_gossipsub_data_column_sidecar ON CLUSTER '{cluster}';

-- Rollback for libp2p_graft

DROP VIEW IF EXISTS mainnet.libp2p_graft ON CLUSTER '{cluster}';

-- Rollback for libp2p_handle_metadata

DROP VIEW IF EXISTS mainnet.libp2p_handle_metadata ON CLUSTER '{cluster}';

-- Rollback for libp2p_handle_status

DROP VIEW IF EXISTS mainnet.libp2p_handle_status ON CLUSTER '{cluster}';

-- Rollback for libp2p_join

DROP VIEW IF EXISTS mainnet.libp2p_join ON CLUSTER '{cluster}';

-- Rollback for libp2p_leave

DROP VIEW IF EXISTS mainnet.libp2p_leave ON CLUSTER '{cluster}';

-- Rollback for libp2p_peer

DROP VIEW IF EXISTS mainnet.libp2p_peer ON CLUSTER '{cluster}';

-- Rollback for libp2p_prune

DROP VIEW IF EXISTS mainnet.libp2p_prune ON CLUSTER '{cluster}';

-- Rollback for libp2p_publish_message

DROP VIEW IF EXISTS mainnet.libp2p_publish_message ON CLUSTER '{cluster}';

-- Rollback for libp2p_recv_rpc

DROP VIEW IF EXISTS mainnet.libp2p_recv_rpc ON CLUSTER '{cluster}';

-- Rollback for libp2p_reject_message

DROP VIEW IF EXISTS mainnet.libp2p_reject_message ON CLUSTER '{cluster}';

-- Rollback for libp2p_remove_peer

DROP VIEW IF EXISTS mainnet.libp2p_remove_peer ON CLUSTER '{cluster}';

-- Rollback for libp2p_rpc_meta_control_graft

DROP VIEW IF EXISTS mainnet.libp2p_rpc_meta_control_graft ON CLUSTER '{cluster}';

-- Rollback for libp2p_rpc_meta_control_idontwant

DROP VIEW IF EXISTS mainnet.libp2p_rpc_meta_control_idontwant ON CLUSTER '{cluster}';

-- Rollback for libp2p_rpc_meta_control_ihave

DROP VIEW IF EXISTS mainnet.libp2p_rpc_meta_control_ihave ON CLUSTER '{cluster}';

-- Rollback for libp2p_rpc_meta_control_iwant

DROP VIEW IF EXISTS mainnet.libp2p_rpc_meta_control_iwant ON CLUSTER '{cluster}';

-- Rollback for libp2p_rpc_meta_control_prune

DROP VIEW IF EXISTS mainnet.libp2p_rpc_meta_control_prune ON CLUSTER '{cluster}';

-- Rollback for libp2p_rpc_meta_message

DROP VIEW IF EXISTS mainnet.libp2p_rpc_meta_message ON CLUSTER '{cluster}';

-- Rollback for libp2p_rpc_meta_subscription

DROP VIEW IF EXISTS mainnet.libp2p_rpc_meta_subscription ON CLUSTER '{cluster}';

-- Rollback for libp2p_send_rpc

DROP VIEW IF EXISTS mainnet.libp2p_send_rpc ON CLUSTER '{cluster}';

-- Rollback for libp2p_synthetic_heartbeat

DROP VIEW IF EXISTS mainnet.libp2p_synthetic_heartbeat ON CLUSTER '{cluster}';

-- Rollback for mempool_dumpster_transaction

DROP VIEW IF EXISTS mainnet.mempool_dumpster_transaction ON CLUSTER '{cluster}';

-- Rollback for mempool_transaction

DROP VIEW IF EXISTS mainnet.mempool_transaction ON CLUSTER '{cluster}';

-- Rollback for mev_relay_bid_trace

DROP VIEW IF EXISTS mainnet.mev_relay_bid_trace ON CLUSTER '{cluster}';

-- Rollback for mev_relay_proposer_payload_delivered

DROP VIEW IF EXISTS mainnet.mev_relay_proposer_payload_delivered ON CLUSTER '{cluster}';

-- Rollback for mev_relay_validator_registration

DROP VIEW IF EXISTS mainnet.mev_relay_validator_registration ON CLUSTER '{cluster}';

-- Rollback for node_record_consensus

DROP VIEW IF EXISTS mainnet.node_record_consensus ON CLUSTER '{cluster}';

-- Rollback for node_record_execution

DROP VIEW IF EXISTS mainnet.node_record_execution ON CLUSTER '{cluster}';
