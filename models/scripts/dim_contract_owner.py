#!/usr/bin/env python3
"""
Contract Owner Data Collection
Collects contract ownership metadata from growthepie, eth-labels, and Dune Analytics
for contracts appearing in the top 100 storage slot tables.

This script:
1. Fetches existing records from dim_contract_owner
2. Fetches data from growthepie API and merges into records
3. Fetches data from eth-labels API and merges into records
4. Fetches data from Dune Analytics API (if API key set) and merges into records
5. Inserts merged records with labels and sources arrays
"""

import os
import sys
import urllib.request
import urllib.error
import urllib.parse
import json
import base64
import time
from datetime import datetime


def execute_clickhouse_query(url, query):
    """Execute a query via ClickHouse HTTP interface with proper auth handling"""
    try:
        parsed = urllib.parse.urlparse(url)

        if parsed.port:
            clean_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}{parsed.path}"
        else:
            clean_url = f"{parsed.scheme}://{parsed.hostname}{parsed.path}"

        if parsed.query:
            clean_url += f"?{parsed.query}"

        req = urllib.request.Request(clean_url,
                                    data=query.encode('utf-8'),
                                    method='POST')

        if parsed.username:
            password = parsed.password or ''
            auth_str = f"{parsed.username}:{password}"
            b64_auth = base64.b64encode(auth_str.encode()).decode()
            req.add_header("Authorization", f"Basic {b64_auth}")

        response = urllib.request.urlopen(req)
        result = response.read().decode('utf-8')

        if result.strip():
            if 'SELECT' in query.upper():
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    return {'data': []}
        return {'data': []}
    except Exception as e:
        print(f"Query failed: {e}", file=sys.stderr)
        print(f"URL: {url}", file=sys.stderr)
        print(f"Query: {query[:200]}...", file=sys.stderr)
        raise


def get_current_top_100_addresses(ch_url, target_db):
    """Get unique contract addresses from all top 100 storage slot tables"""
    query = f"""
    SELECT DISTINCT contract_address
    FROM (
        SELECT contract_address FROM `{target_db}`.fct_storage_slot_top_100_by_slots FINAL
        UNION DISTINCT
        SELECT contract_address FROM `{target_db}`.fct_storage_slot_top_100_by_bytes FINAL
    )
    FORMAT JSONCompact
    """

    result = execute_clickhouse_query(ch_url, query)
    addresses = set()
    if result and 'data' in result:
        for row in result['data']:
            addresses.add(row[0].lower())
    return addresses


def get_existing_records(ch_url, target_db, target_table):
    """Get all existing records from dim_contract_owner as a dict keyed by address"""
    query = f"""
    SELECT
        contract_address,
        owner_key,
        account_owner,
        contract_name,
        factory_contract,
        labels,
        sources
    FROM `{target_db}`.`{target_table}` FINAL
    FORMAT JSONCompact
    """

    try:
        result = execute_clickhouse_query(ch_url, query)
        records = {}
        if result and 'data' in result:
            for row in result['data']:
                address = row[0].lower()
                records[address] = {
                    'owner_key': row[1],
                    'account_owner': row[2],
                    'contract_name': row[3],
                    'factory_contract': row[4],
                    'labels': row[5] if row[5] else [],
                    'sources': row[6] if row[6] else [],
                }
        return records
    except Exception as e:
        print(f"Warning: Could not query existing records: {e}", file=sys.stderr)
        return {}


def map_network_to_blockchain(network):
    """Map network name to Dune/growthepie blockchain identifier"""
    mapping = {
        'mainnet': 'ethereum',
        'ethereum': 'ethereum',
        'hoodi': 'hoodi',
        'sepolia': 'sepolia',
        'holesky': 'holesky',
    }
    return mapping.get(network.lower(), 'ethereum')


def get_network_chain_id(network):
    """Map network name to eth-labels chainId"""
    mapping = {
        'mainnet': 1,
        'ethereum': 1,
        'optimism': 10,
        'base': 8453,
        'arbitrum': 42161,
        'gnosis': 100,
        'bsc': 56,
        'celo': 42220,
    }
    return mapping.get(network.lower())


def fetch_growthepie_data(blockchain):
    """Fetch contract labels from growthepie API filtered by blockchain"""
    url = "https://api.growthepie.com/v1/labels/full.json"

    try:
        print(f"  Fetching growthepie data...")
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        response = urllib.request.urlopen(req, timeout=120)
        data = json.loads(response.read().decode('utf-8'))

        # Extract types (column names) and data
        types = data.get('data', {}).get('types', [])
        rows = data.get('data', {}).get('data', [])

        # Find column indices
        idx_address = types.index('address') if 'address' in types else None
        idx_origin_key = types.index('origin_key') if 'origin_key' in types else None
        idx_name = types.index('name') if 'name' in types else None
        idx_owner_project = types.index('owner_project') if 'owner_project' in types else None
        idx_owner_project_clear = types.index('owner_project_clear') if 'owner_project_clear' in types else None
        idx_deployer_address = types.index('deployer_address') if 'deployer_address' in types else None
        idx_usage_category = types.index('usage_category') if 'usage_category' in types else None

        if idx_address is None or idx_origin_key is None:
            print("  Warning: Required columns not found in growthepie data", file=sys.stderr)
            return {}

        # Filter by blockchain and extract relevant fields
        results = {}
        for row in rows:
            origin_key = row[idx_origin_key] if idx_origin_key is not None else None
            if origin_key != blockchain:
                continue

            address = row[idx_address] if idx_address is not None else None
            if not address:
                continue

            address = address.lower()
            usage_category = row[idx_usage_category] if idx_usage_category is not None else None

            results[address] = {
                'owner_key': row[idx_owner_project] if idx_owner_project is not None else None,
                'account_owner': row[idx_owner_project_clear] if idx_owner_project_clear is not None else None,
                'contract_name': row[idx_name] if idx_name is not None else None,
                'factory_contract': row[idx_deployer_address] if idx_deployer_address is not None else None,
                'labels': [usage_category] if usage_category else [],
                'sources': ['growthepie'],
            }

        print(f"  Found {len(results)} records for {blockchain}")
        return results

    except Exception as e:
        print(f"Warning: Failed to fetch growthepie data: {e}", file=sys.stderr)
        return {}


def fetch_eth_labels_data(addresses, chain_id):
    """Fetch labels from eth-labels API (one address at a time)"""
    if not chain_id:
        return {}

    results = {}
    total = len(addresses)
    processed = 0

    for addr in addresses:
        try:
            url = f"https://eth-labels-production.up.railway.app/accounts?chainId={chain_id}&address={addr}"
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/json")
            response = urllib.request.urlopen(req, timeout=30)
            data = json.loads(response.read().decode('utf-8'))

            if data and isinstance(data, list) and len(data) > 0:
                # Aggregate all labels for this address
                labels = []
                contract_name = None
                for item in data:
                    label = item.get('label')
                    if label and label not in labels:
                        labels.append(label)
                    if not contract_name:
                        contract_name = item.get('nameTag')

                if labels:
                    results[addr.lower()] = {
                        'owner_key': None,
                        'account_owner': None,
                        'contract_name': contract_name,
                        'factory_contract': None,
                        'labels': labels,
                        'sources': ['eth-labels'],
                    }

            processed += 1
            if processed % 50 == 0:
                print(f"    Processed {processed}/{total} addresses...")

            # Rate limiting - small delay between requests
            time.sleep(0.1)

        except urllib.error.HTTPError as e:
            if e.code == 404:
                # Address not found in eth-labels, skip
                pass
            else:
                print(f"Warning: eth-labels API error for {addr}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Failed to fetch eth-labels for {addr}: {e}", file=sys.stderr)

    print(f"  Found {len(results)} records from eth-labels")
    return results


def query_dune_api(api_key, addresses, blockchain):
    """Query Dune Analytics API for contract ownership data"""
    if not addresses:
        return {}

    # Format addresses for SQL IN clause (Dune expects 0x prefix)
    formatted_addresses = ", ".join([f"0x{addr[2:]}" if addr.startswith('0x') else f"0x{addr}" for addr in addresses])

    sql_query = f"""
    SELECT
        address,
        owner_key,
        custody_owner,
        contract_name,
        factory_contract
    FROM labels.owner_addresses
    WHERE
        "address" IN ({formatted_addresses})
        AND blockchain = '{blockchain}'
    """

    # Step 1: Execute the query
    execute_url = "https://api.dune.com/api/v1/sql/execute"
    headers = {
        "X-Dune-API-Key": api_key,
        "Content-Type": "application/json"
    }

    payload = json.dumps({
        "sql": sql_query,
        "performance": "medium"
    }).encode('utf-8')

    try:
        req = urllib.request.Request(execute_url, data=payload, headers=headers, method='POST')
        response = urllib.request.urlopen(req, timeout=60)
        result = json.loads(response.read().decode('utf-8'))
        execution_id = result.get('execution_id')

        if not execution_id:
            print(f"Warning: No execution_id in response: {result}", file=sys.stderr)
            return {}

        print(f"  Dune query submitted, execution_id: {execution_id}")

    except Exception as e:
        print(f"Warning: Failed to submit Dune query: {e}", file=sys.stderr)
        return {}

    # Step 2: Poll for completion
    status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
    max_attempts = 60  # 5 minutes with 5 second intervals
    attempt = 0

    while attempt < max_attempts:
        try:
            req = urllib.request.Request(status_url, headers={"X-Dune-API-Key": api_key})
            response = urllib.request.urlopen(req, timeout=30)
            status_result = json.loads(response.read().decode('utf-8'))
            state = status_result.get('state', '')

            if state == 'QUERY_STATE_COMPLETED':
                print(f"  Dune query completed")
                break
            elif state in ['QUERY_STATE_FAILED', 'QUERY_STATE_CANCELLED']:
                print(f"Warning: Dune query failed with state: {state}", file=sys.stderr)
                return {}

            time.sleep(5)
            attempt += 1

        except Exception as e:
            print(f"Warning: Failed to check Dune status: {e}", file=sys.stderr)
            time.sleep(5)
            attempt += 1

    if attempt >= max_attempts:
        print("Warning: Dune query timed out", file=sys.stderr)
        return {}

    # Step 3: Fetch results
    results_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"

    try:
        req = urllib.request.Request(results_url, headers={"X-Dune-API-Key": api_key})
        response = urllib.request.urlopen(req, timeout=60)
        results = json.loads(response.read().decode('utf-8'))

        rows = results.get('result', {}).get('rows', [])
        print(f"  Dune returned {len(rows)} owner records")

        # Convert to dict keyed by address
        result_dict = {}
        for row in rows:
            address = str(row.get('address', ''))
            if address.startswith('\\x'):
                address = '0x' + address[2:]
            address = address.lower()

            result_dict[address] = {
                'owner_key': row.get('owner_key'),
                'account_owner': row.get('custody_owner'),
                'contract_name': row.get('contract_name'),
                'factory_contract': row.get('factory_contract'),
                'labels': [],  # Dune doesn't provide labels
                'sources': ['dune'],
            }

        return result_dict

    except Exception as e:
        print(f"Warning: Failed to fetch Dune results: {e}", file=sys.stderr)
        return {}


def merge_records(existing, new_records, source_name=None):
    """Merge new records into existing, combining arrays and coalescing fields"""
    for addr, new_data in new_records.items():
        addr = addr.lower()
        if addr in existing:
            # Merge sources
            existing_sources = existing[addr].get('sources', [])
            new_sources = new_data.get('sources', [])
            if source_name and source_name not in existing_sources:
                existing_sources.append(source_name)
            for src in new_sources:
                if src not in existing_sources:
                    existing_sources.append(src)
            existing[addr]['sources'] = sorted(existing_sources)

            # Merge labels
            existing_labels = existing[addr].get('labels', [])
            new_labels = new_data.get('labels', [])
            for lbl in new_labels:
                if lbl and lbl not in existing_labels:
                    existing_labels.append(lbl)
            existing[addr]['labels'] = sorted(existing_labels)

            # Coalesce nullable fields (prefer non-null, keep existing if already set)
            for field in ['owner_key', 'account_owner', 'contract_name', 'factory_contract']:
                if not existing[addr].get(field) and new_data.get(field):
                    existing[addr][field] = new_data[field]
        else:
            existing[addr] = {
                'owner_key': new_data.get('owner_key'),
                'account_owner': new_data.get('account_owner'),
                'contract_name': new_data.get('contract_name'),
                'factory_contract': new_data.get('factory_contract'),
                'labels': sorted(new_data.get('labels', [])),
                'sources': sorted(new_data.get('sources', [])),
            }

    return existing


def format_array_literal(arr):
    """Format a Python list as a ClickHouse array literal"""
    if not arr:
        return '[]'
    escaped = [str(v).replace("'", "''") for v in arr if v]
    return "['" + "', '".join(escaped) + "']"


def insert_records(ch_url, target_db, target_table, records, task_start):
    """Insert records into dim_contract_owner with array columns"""
    if not records:
        return 0

    values = []
    for address, row in records.items():
        address_escaped = address.replace("'", "''")

        # Helper to escape nullable string fields
        def escape_nullable(val):
            if val is None:
                return 'NULL'
            return "'" + str(val).replace("'", "''") + "'"

        labels_literal = format_array_literal(row.get('labels', []))
        sources_literal = format_array_literal(row.get('sources', []))

        values.append(f"""(
            fromUnixTimestamp({task_start}),
            '{address_escaped}',
            {escape_nullable(row.get('owner_key'))},
            {escape_nullable(row.get('account_owner'))},
            {escape_nullable(row.get('contract_name'))},
            {escape_nullable(row.get('factory_contract'))},
            {labels_literal},
            {sources_literal}
        )""")

    # Batch insert
    chunk_size = 100
    inserted_count = 0
    try:
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i + chunk_size]
            insert_query = f"""
            INSERT INTO `{target_db}`.`{target_table}`
            (updated_date_time, contract_address, owner_key, account_owner,
             contract_name, factory_contract, labels, sources)
            VALUES {','.join(chunk)}
            """
            execute_clickhouse_query(ch_url, insert_query)
            inserted_count += len(chunk)

        return inserted_count

    except Exception as e:
        print(f"ERROR: Failed to insert data: {e}", file=sys.stderr)
        print(f"Inserted {inserted_count} out of {len(records)} rows before failure", file=sys.stderr)
        raise


def main():
    ch_url = os.environ['CLICKHOUSE_URL']
    target_db = os.environ['SELF_DATABASE']
    target_table = os.environ['SELF_TABLE']
    task_start = os.environ.get('TASK_START', str(int(datetime.now().timestamp())))
    network = os.environ.get('NETWORK', target_db)
    dune_api_key = os.environ.get('DUNE_API_KEY')  # Optional

    print(f"=== Contract Owner Data Collection ===")
    print(f"Target: {target_db}.{target_table}")
    print(f"Network: {network}")

    try:
        # Test ClickHouse connection
        print("Testing ClickHouse connection...")
        test_query = "SELECT 1 FORMAT JSONCompact"
        try:
            execute_clickhouse_query(ch_url, test_query)
            print("  ClickHouse connection successful")
        except Exception as e:
            print(f"ERROR: Cannot connect to ClickHouse: {e}", file=sys.stderr)
            return 1

        blockchain = map_network_to_blockchain(network)
        chain_id = get_network_chain_id(network)

        # =====================================================================
        # Step 1: Get existing records and top 100 addresses
        # =====================================================================
        print(f"\n=== Fetching Existing Data ===")

        print("Fetching contract addresses from top 100 tables...")
        top_100_addresses = get_current_top_100_addresses(ch_url, target_db)
        print(f"  Found {len(top_100_addresses)} unique addresses in top 100 tables")

        print("Fetching existing records from dim_contract_owner...")
        all_records = get_existing_records(ch_url, target_db, target_table)
        print(f"  Found {len(all_records)} existing records")

        # =====================================================================
        # Step 2: Fetch and merge growthepie data
        # =====================================================================
        print(f"\n=== Growthepie Data Collection ===")
        print(f"Fetching growthepie data for {blockchain}...")

        growthepie_records = fetch_growthepie_data(blockchain)
        if growthepie_records:
            all_records = merge_records(all_records, growthepie_records)
            print(f"  Merged growthepie data, total records: {len(all_records)}")

        # =====================================================================
        # Step 3: Fetch and merge eth-labels data
        # =====================================================================
        print(f"\n=== eth-labels Data Collection ===")
        if chain_id:
            print(f"Fetching eth-labels data for chainId {chain_id}...")
            # Only query addresses we care about (top 100 + existing)
            addresses_to_query = top_100_addresses | set(all_records.keys())
            eth_labels_records = fetch_eth_labels_data(list(addresses_to_query), chain_id)
            if eth_labels_records:
                all_records = merge_records(all_records, eth_labels_records)
                print(f"  Merged eth-labels data, total records: {len(all_records)}")
        else:
            print(f"  Skipping: chainId not supported for network '{network}'")

        # =====================================================================
        # Step 4: Fetch and merge Dune data
        # =====================================================================
        print(f"\n=== Dune Data Collection ===")
        if not dune_api_key:
            print("  Skipping: DUNE_API_KEY not set")
        else:
            # Query Dune for top 100 addresses that don't have dune as a source yet
            addresses_needing_dune = [
                addr for addr in top_100_addresses
                if addr not in all_records or 'dune' not in all_records[addr].get('sources', [])
            ]

            if addresses_needing_dune:
                print(f"Querying Dune Analytics for {len(addresses_needing_dune)} addresses on {blockchain}...")

                # Batch addresses to avoid query size limits
                batch_size = 100
                for i in range(0, len(addresses_needing_dune), batch_size):
                    batch = addresses_needing_dune[i:i + batch_size]
                    print(f"  Processing batch {i // batch_size + 1}/{(len(addresses_needing_dune) + batch_size - 1) // batch_size}...")
                    dune_records = query_dune_api(dune_api_key, batch, blockchain)

                    if dune_records:
                        all_records = merge_records(all_records, dune_records)

                    # Rate limiting between batches
                    if i + batch_size < len(addresses_needing_dune):
                        time.sleep(2)

                print(f"  Merged Dune data, total records: {len(all_records)}")
            else:
                print("  No new addresses need Dune lookup")

        # =====================================================================
        # Step 5: Insert all records
        # =====================================================================
        print(f"\n=== Inserting Records ===")

        # Filter to only records that have at least one source
        records_to_insert = {
            addr: data for addr, data in all_records.items()
            if data.get('sources')
        }

        if not records_to_insert:
            print("No records to insert.")
            return 0

        print(f"Inserting {len(records_to_insert)} records into ClickHouse...")
        inserted = insert_records(ch_url, target_db, target_table, records_to_insert, task_start)
        print(f"  Successfully inserted {inserted} records")

        print("\nContract owner data collection completed successfully")
        return 0

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
