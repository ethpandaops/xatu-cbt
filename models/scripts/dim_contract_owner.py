#!/usr/bin/env python3
"""
Contract Owner Data Collection
Collects contract ownership metadata from growthepie and Dune Analytics for contracts
appearing in the top 100 storage slot tables.

This script:
1. Fetches growthepie data and inserts new records not already in dim_contract_owner
2. Queries unique contract addresses from all 4 top 100 storage slot tables
3. Checks which addresses are already in dim_contract_owner
4. Queries Dune Analytics API for ownership data on new addresses only
5. Inserts results into dim_contract_owner (accumulates over time)
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


def get_existing_addresses(ch_url, target_db, target_table):
    """Get addresses already in dim_contract_owner"""
    query = f"""
    SELECT DISTINCT contract_address
    FROM `{target_db}`.`{target_table}` FINAL
    FORMAT JSONCompact
    """

    try:
        result = execute_clickhouse_query(ch_url, query)
        addresses = set()
        if result and 'data' in result:
            for row in result['data']:
                addresses.add(row[0].lower())
        return addresses
    except Exception as e:
        print(f"Warning: Could not query existing addresses: {e}", file=sys.stderr)
        return set()


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
            return []

        # Filter by blockchain and extract relevant fields
        results = []
        for row in rows:
            origin_key = row[idx_origin_key] if idx_origin_key is not None else None
            if origin_key != blockchain:
                continue

            address = row[idx_address] if idx_address is not None else None
            if not address:
                continue

            results.append({
                'address': address.lower(),
                'owner_key': row[idx_owner_project] if idx_owner_project is not None else None,
                'account_owner': row[idx_owner_project_clear] if idx_owner_project_clear is not None else None,
                'contract_name': row[idx_name] if idx_name is not None else None,
                'factory_contract': row[idx_deployer_address] if idx_deployer_address is not None else None,
                'usage_category': row[idx_usage_category] if idx_usage_category is not None else None,
            })

        print(f"  Found {len(results)} records for {blockchain}")
        return results

    except Exception as e:
        print(f"Warning: Failed to fetch growthepie data: {e}", file=sys.stderr)
        return []


def query_dune_api(api_key, addresses, blockchain):
    """Query Dune Analytics API for contract ownership data"""
    if not addresses:
        return []

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
            return []

        print(f"  Dune query submitted, execution_id: {execution_id}")

    except Exception as e:
        print(f"Warning: Failed to submit Dune query: {e}", file=sys.stderr)
        return []

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
                return []

            time.sleep(5)
            attempt += 1

        except Exception as e:
            print(f"Warning: Failed to check Dune status: {e}", file=sys.stderr)
            time.sleep(5)
            attempt += 1

    if attempt >= max_attempts:
        print("Warning: Dune query timed out", file=sys.stderr)
        return []

    # Step 3: Fetch results
    results_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"

    try:
        req = urllib.request.Request(results_url, headers={"X-Dune-API-Key": api_key})
        response = urllib.request.urlopen(req, timeout=60)
        results = json.loads(response.read().decode('utf-8'))

        rows = results.get('result', {}).get('rows', [])
        print(f"  Dune returned {len(rows)} owner records")
        return rows

    except Exception as e:
        print(f"Warning: Failed to fetch Dune results: {e}", file=sys.stderr)
        return []


def insert_records(ch_url, target_db, target_table, records, source, task_start):
    """Insert records into dim_contract_owner"""
    if not records:
        return 0

    values = []
    for row in records:
        # Get address and normalize to lowercase with 0x prefix
        address = str(row.get('address', ''))
        if address.startswith('\\x'):
            address = '0x' + address[2:]
        address = address.lower()
        address_escaped = address.replace("'", "''")

        # Helper to escape nullable string fields
        def escape_nullable(val):
            if val is None:
                return 'NULL'
            return "'" + str(val).replace("'", "''") + "'"

        values.append(f"""(
            fromUnixTimestamp({task_start}),
            '{address_escaped}',
            {escape_nullable(row.get('owner_key'))},
            {escape_nullable(row.get('account_owner'))},
            {escape_nullable(row.get('contract_name'))},
            {escape_nullable(row.get('factory_contract'))},
            {escape_nullable(row.get('usage_category'))},
            '{source}'
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
             contract_name, factory_contract, usage_category, source)
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

        # =====================================================================
        # Step 1: Fetch and insert growthepie data
        # =====================================================================
        print(f"\n=== Growthepie Data Collection ===")
        print(f"Fetching growthepie data for {blockchain}...")

        growthepie_records = fetch_growthepie_data(blockchain)

        if growthepie_records:
            # Get existing addresses
            print("Checking existing addresses in dim_contract_owner...")
            existing_addresses = get_existing_addresses(ch_url, target_db, target_table)
            print(f"  Found {len(existing_addresses)} existing addresses")

            # Filter to new addresses only
            new_growthepie_records = [
                r for r in growthepie_records
                if r['address'].lower() not in existing_addresses
            ]
            print(f"  New addresses from growthepie: {len(new_growthepie_records)}")

            if new_growthepie_records:
                print(f"Inserting {len(new_growthepie_records)} growthepie records...")
                inserted = insert_records(
                    ch_url, target_db, target_table,
                    new_growthepie_records, 'growthepie', task_start
                )
                print(f"  Successfully inserted {inserted} growthepie records")
        else:
            print("  No growthepie records found")

        # =====================================================================
        # Step 2: Fetch and insert Dune data for top 100 addresses
        # =====================================================================
        if not dune_api_key:
            print(f"\n=== Dune Data Collection ===")
            print("  Skipping: DUNE_API_KEY not set")
            print("\nContract owner data collection completed successfully")
            return 0

        print(f"\n=== Dune Data Collection ===")

        # Get current top 100 addresses
        print("Fetching contract addresses from top 100 tables...")
        current_addresses = get_current_top_100_addresses(ch_url, target_db)
        print(f"  Found {len(current_addresses)} unique addresses in top 100 tables")

        if not current_addresses:
            print("No addresses found in top 100 tables. Exiting.")
            return 0

        # Get existing addresses (refresh after growthepie inserts)
        print("Checking existing addresses in dim_contract_owner...")
        existing_addresses = get_existing_addresses(ch_url, target_db, target_table)
        print(f"  Found {len(existing_addresses)} existing addresses")

        # Compute new addresses
        new_addresses = current_addresses - existing_addresses
        print(f"  New addresses to lookup in Dune: {len(new_addresses)}")

        if not new_addresses:
            print("\nNo new addresses to process. Exiting successfully.")
            return 0

        # Query Dune API
        print(f"\nQuerying Dune Analytics for {len(new_addresses)} addresses on {blockchain}...")

        # Batch addresses to avoid query size limits
        batch_size = 100
        all_results = []
        address_list = list(new_addresses)

        for i in range(0, len(address_list), batch_size):
            batch = address_list[i:i + batch_size]
            print(f"  Processing batch {i // batch_size + 1}/{(len(address_list) + batch_size - 1) // batch_size}...")
            results = query_dune_api(dune_api_key, batch, blockchain)

            # Map Dune fields to our schema
            for row in results:
                all_results.append({
                    'address': row.get('address', ''),
                    'owner_key': row.get('owner_key'),
                    'account_owner': row.get('custody_owner'),  # custody_owner -> account_owner
                    'contract_name': row.get('contract_name'),
                    'factory_contract': row.get('factory_contract'),
                    'usage_category': None,  # Not available from Dune
                })

            # Rate limiting between batches
            if i + batch_size < len(address_list):
                time.sleep(2)

        print(f"\nTotal owner records from Dune: {len(all_results)}")

        if not all_results:
            print("No owner data found in Dune for the new addresses. Exiting.")
            return 0

        # Insert Dune data
        print(f"\nInserting {len(all_results)} Dune records into ClickHouse...")
        inserted = insert_records(
            ch_url, target_db, target_table,
            all_results, 'dune', task_start
        )
        print(f"  Successfully inserted {inserted} Dune records")

        print("\nContract owner data collection completed successfully")
        return 0

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
