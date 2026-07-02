#!/usr/bin/env python3
"""
Blob Submitter Label Collection

Resolves human-readable names for blob submitter (L1 batcher) addresses so that
`dim_block_blob_submitter` can label rollups instead of falling back to 'Unknown'.

This script:
1. Finds the set of addresses that have recently submitted blob transactions
2. Fetches existing records from dim_blob_submitter (to preserve prior labels)
3. Enriches addresses from growthepie, eth-labels, and Dune Analytics (if a key is set)
4. Resolves a single best name per address and inserts (address, name, labels, sources)

It mirrors models/scripts/dim_contract_owner.py, which follows the same
label-collection pattern for contract owners.
"""

import os
import sys
import json
import time
import base64
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Higher number wins when more than one source proposes a name for an address.
# Dune's owner labels are the most authoritative for rollup batchers.
SOURCE_PRIORITY = {'dune': 3, 'growthepie': 2, 'eth-labels': 1}

# How far back (in blocks) to look for active blob submitters. ~90 days at 12s slots.
DEFAULT_LOOKBACK_BLOCKS = 650000


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


def get_blob_submitter_addresses(ch_url, target_db, network, lookback_blocks):
    """Get distinct recent blob submitter (from) addresses for the network"""
    query = f"""
    SELECT DISTINCT lower(`from`) AS address
    FROM `{target_db}`.execution_transaction
    WHERE `type` = 3
      AND success = true
      AND meta_network_name = '{network}'
      AND block_number >= (
        SELECT max(block_number) - {lookback_blocks}
        FROM `{target_db}`.execution_transaction
        WHERE meta_network_name = '{network}'
      )
    FORMAT JSONCompact
    """

    result = execute_clickhouse_query(ch_url, query)
    addresses = set()
    if result and 'data' in result:
        for row in result['data']:
            if row and row[0]:
                addresses.add(row[0].lower())
    return addresses


def get_existing_records(ch_url, target_db, target_table):
    """Get all existing records from dim_blob_submitter as a dict keyed by address"""
    query = f"""
    SELECT address, name, labels, sources
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
                    'name': row[1],
                    'labels': row[2] if row[2] else [],
                    'sources': row[3] if row[3] else [],
                    # Prior name is treated as a growthepie-tier candidate so a
                    # fresh higher-priority source can still override it.
                    'name_priority': SOURCE_PRIORITY['growthepie'] if row[1] else 0,
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
        'sepolia': 11155111,
        'holesky': 17000,
    }
    return mapping.get(network.lower())


def fetch_growthepie_data(blockchain):
    """Fetch labels from growthepie filtered by blockchain, keyed by address"""
    url = "https://api.growthepie.com/v1/labels/full.json"

    try:
        print("  Fetching growthepie data...")
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        response = urllib.request.urlopen(req, timeout=120)
        data = json.loads(response.read().decode('utf-8'))

        types = data.get('data', {}).get('types', [])
        rows = data.get('data', {}).get('data', [])

        idx_address = types.index('address') if 'address' in types else None
        idx_origin_key = types.index('origin_key') if 'origin_key' in types else None
        idx_name = types.index('name') if 'name' in types else None
        idx_owner_clear = types.index('owner_project_clear') if 'owner_project_clear' in types else None
        idx_usage_category = types.index('usage_category') if 'usage_category' in types else None

        if idx_address is None or idx_origin_key is None:
            print("  Warning: Required columns not found in growthepie data", file=sys.stderr)
            return {}

        results = {}
        for row in rows:
            if row[idx_origin_key] != blockchain:
                continue
            address = row[idx_address]
            if not address:
                continue

            name = None
            if idx_owner_clear is not None:
                name = row[idx_owner_clear]
            if not name and idx_name is not None:
                name = row[idx_name]

            usage_category = row[idx_usage_category] if idx_usage_category is not None else None

            results[address.lower()] = {
                'name': name,
                'labels': [usage_category] if usage_category else [],
                'sources': ['growthepie'],
            }

        print(f"  Found {len(results)} records for {blockchain}")
        return results

    except Exception as e:
        print(f"Warning: Failed to fetch growthepie data: {e}", file=sys.stderr)
        return {}


def fetch_eth_labels_data(addresses, chain_id, max_workers=100):
    """Fetch labels from eth-labels API with concurrent requests"""
    if not chain_id or not addresses:
        return {}

    print(f"  Querying eth-labels for {len(addresses)} addresses (max {max_workers} concurrent)...")

    def fetch_single(addr):
        try:
            url = f"https://eth-labels-production.up.railway.app/accounts?chainId={chain_id}&address={addr}"
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/json")
            response = urllib.request.urlopen(req, timeout=30)
            data = json.loads(response.read().decode('utf-8'))

            if data and isinstance(data, list) and len(data) > 0:
                labels = []
                name = None
                for item in data:
                    label = item.get('label')
                    if label and label not in labels:
                        labels.append(label)
                    if not name:
                        name = item.get('nameTag')

                if labels or name:
                    return addr.lower(), {
                        'name': name,
                        'labels': labels,
                        'sources': ['eth-labels'],
                    }
            return addr.lower(), None

        except urllib.error.HTTPError as e:
            if e.code != 404:
                print(f"Warning: eth-labels API error for {addr}: {e}", file=sys.stderr)
            return addr.lower(), None
        except Exception as e:
            print(f"Warning: Failed to fetch eth-labels for {addr}: {e}", file=sys.stderr)
            return addr.lower(), None

    results = {}
    completed = 0
    total = len(addresses)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_single, addr): addr for addr in addresses}
        for future in as_completed(futures):
            addr, data = future.result()
            if data:
                results[addr] = data
            completed += 1
            if completed % 50 == 0 or completed == total:
                print(f"    Processed {completed}/{total} addresses...")

    print(f"  Found {len(results)} records from eth-labels")
    return results


def query_dune_api(api_key, addresses, blockchain):
    """Query Dune Analytics labels.owner_addresses for blob submitter labels"""
    if not addresses:
        return {}

    formatted_addresses = ", ".join(
        [f"0x{addr[2:]}" if addr.startswith('0x') else f"0x{addr}" for addr in addresses]
    )

    sql_query = f"""
    SELECT
        address,
        owner_key,
        custody_owner,
        contract_name
    FROM labels.owner_addresses
    WHERE
        "address" IN ({formatted_addresses})
        AND blockchain = '{blockchain}'
    """

    execute_url = "https://api.dune.com/api/v1/sql/execute"
    headers = {"X-Dune-API-Key": api_key, "Content-Type": "application/json"}
    payload = json.dumps({"sql": sql_query, "performance": "medium"}).encode('utf-8')

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

    status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
    max_attempts = 60
    attempt = 0
    while attempt < max_attempts:
        try:
            req = urllib.request.Request(status_url, headers={"X-Dune-API-Key": api_key})
            response = urllib.request.urlopen(req, timeout=30)
            state = json.loads(response.read().decode('utf-8')).get('state', '')
            if state == 'QUERY_STATE_COMPLETED':
                print("  Dune query completed")
                break
            if state in ['QUERY_STATE_FAILED', 'QUERY_STATE_CANCELLED']:
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

    results_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
    try:
        req = urllib.request.Request(results_url, headers={"X-Dune-API-Key": api_key})
        response = urllib.request.urlopen(req, timeout=60)
        rows = json.loads(response.read().decode('utf-8')).get('result', {}).get('rows', [])
        print(f"  Dune returned {len(rows)} owner records")

        result_dict = {}
        for row in rows:
            address = str(row.get('address', ''))
            if address.startswith('\\x'):
                address = '0x' + address[2:]
            address = address.lower()
            name = row.get('custody_owner') or row.get('contract_name') or row.get('owner_key')
            result_dict[address] = {
                'name': name,
                'labels': [],
                'sources': ['dune'],
            }
        return result_dict

    except Exception as e:
        print(f"Warning: Failed to fetch Dune results: {e}", file=sys.stderr)
        return {}


def merge_records(existing, new_records):
    """Merge new records into existing, combining arrays and resolving name by source priority"""
    for addr, new_data in new_records.items():
        addr = addr.lower()
        new_sources = new_data.get('sources', [])
        new_priority = max((SOURCE_PRIORITY.get(s, 0) for s in new_sources), default=0)

        if addr not in existing:
            existing[addr] = {'name': None, 'labels': [], 'sources': [], 'name_priority': 0}

        record = existing[addr]

        for src in new_sources:
            if src not in record['sources']:
                record['sources'].append(src)
        record['sources'] = sorted(record['sources'])

        for lbl in new_data.get('labels', []):
            if lbl and lbl not in record['labels']:
                record['labels'].append(lbl)
        record['labels'] = sorted(record['labels'])

        new_name = new_data.get('name')
        if new_name and new_priority >= record.get('name_priority', 0):
            record['name'] = new_name
            record['name_priority'] = new_priority

    return existing


def format_array_literal(arr):
    """Format a Python list as a ClickHouse array literal"""
    if not arr:
        return '[]'
    escaped = [str(v).replace("'", "''") for v in arr if v]
    return "['" + "', '".join(escaped) + "']"


def insert_records(ch_url, target_db, target_table, records, task_start):
    """Insert records into dim_blob_submitter"""
    if not records:
        return 0

    def escape_nullable(val):
        if val is None:
            return 'NULL'
        return "'" + str(val).replace("'", "''") + "'"

    values = []
    for address, row in records.items():
        values.append(f"""(
            fromUnixTimestamp({task_start}),
            '{address.replace("'", "''")}',
            {escape_nullable(row.get('name'))},
            {format_array_literal(row.get('labels', []))},
            {format_array_literal(row.get('sources', []))}
        )""")

    chunk_size = 100
    inserted_count = 0
    try:
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i + chunk_size]
            insert_query = f"""
            INSERT INTO `{target_db}`.`{target_table}`
            (updated_date_time, address, name, labels, sources)
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
    lookback_blocks = int(os.environ.get('BLOB_SUBMITTER_LOOKBACK_BLOCKS', DEFAULT_LOOKBACK_BLOCKS))

    print("=== Blob Submitter Label Collection ===")
    print(f"Target: {target_db}.{target_table}")
    print(f"Network: {network}")

    try:
        print("Testing ClickHouse connection...")
        try:
            execute_clickhouse_query(ch_url, "SELECT 1 FORMAT JSONCompact")
            print("  ClickHouse connection successful")
        except Exception as e:
            print(f"ERROR: Cannot connect to ClickHouse: {e}", file=sys.stderr)
            return 1

        blockchain = map_network_to_blockchain(network)
        chain_id = get_network_chain_id(network)

        print("\n=== Fetching Blob Submitter Addresses ===")
        submitter_addresses = get_blob_submitter_addresses(ch_url, target_db, network, lookback_blocks)
        print(f"  Found {len(submitter_addresses)} distinct recent blob submitters")

        if not submitter_addresses:
            print("No blob submitters found; nothing to do.")
            return 0

        print("Fetching existing records from dim_blob_submitter...")
        all_records = get_existing_records(ch_url, target_db, target_table)
        print(f"  Found {len(all_records)} existing records")

        # Growthepie: bulk label set for the chain (filtered to our submitters).
        print("\n=== Growthepie Data Collection ===")
        growthepie_records = fetch_growthepie_data(blockchain)
        growthepie_records = {
            addr: data for addr, data in growthepie_records.items() if addr in submitter_addresses
        }
        if growthepie_records:
            all_records = merge_records(all_records, growthepie_records)
            print(f"  Merged growthepie data for {len(growthepie_records)} submitters")

        # eth-labels: per-address lookup for submitters not yet sourced from it.
        print("\n=== eth-labels Data Collection ===")
        if chain_id:
            needing = [
                addr for addr in submitter_addresses
                if addr not in all_records or 'eth-labels' not in all_records[addr].get('sources', [])
            ]
            if needing:
                eth_labels_records = fetch_eth_labels_data(needing, chain_id)
                if eth_labels_records:
                    all_records = merge_records(all_records, eth_labels_records)
                    print(f"  Merged eth-labels data, total records: {len(all_records)}")
            else:
                print("  No new addresses need eth-labels lookup")
        else:
            print(f"  Skipping: chainId not supported for network '{network}'")

        # Dune: authoritative owner labels for rollup batchers.
        print("\n=== Dune Data Collection ===")
        if not dune_api_key:
            print("  Skipping: DUNE_API_KEY not set")
        else:
            needing = [
                addr for addr in submitter_addresses
                if addr not in all_records or 'dune' not in all_records[addr].get('sources', [])
            ]
            if needing:
                batch_size = 100
                for i in range(0, len(needing), batch_size):
                    batch = needing[i:i + batch_size]
                    print(f"  Processing batch {i // batch_size + 1}/{(len(needing) + batch_size - 1) // batch_size}...")
                    dune_records = query_dune_api(dune_api_key, batch, blockchain)
                    if dune_records:
                        all_records = merge_records(all_records, dune_records)
                    if i + batch_size < len(needing):
                        time.sleep(2)
                print(f"  Merged Dune data, total records: {len(all_records)}")
            else:
                print("  No new addresses need Dune lookup")

        print("\n=== Inserting Records ===")
        records_to_insert = {
            addr: data for addr, data in all_records.items() if data.get('sources')
        }
        if not records_to_insert:
            print("No records to insert.")
            return 0

        print(f"Inserting {len(records_to_insert)} records into ClickHouse...")
        inserted = insert_records(ch_url, target_db, target_table, records_to_insert, task_start)
        print(f"  Successfully inserted {inserted} records")

        print("\nBlob submitter label collection completed successfully")
        return 0

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
