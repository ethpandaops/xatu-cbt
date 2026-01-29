#!/usr/bin/env python3
"""
Function Signature Data Collection
Collects function signature lookups from Sourcify's 4byte signature database.

This script:
1. Queries distinct function selectors from int_transaction_call_frame
2. Queries existing selectors from dim_function_signature
3. Computes the difference (new selectors to lookup)
4. Batch lookups new selectors against Sourcify API
5. Inserts new entries to dim_function_signature
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


def get_seen_selectors(ch_url, target_db):
    """Get distinct function selectors from int_transaction_call_frame table"""
    query = f"""
    SELECT DISTINCT function_selector as selector
    FROM `{target_db}`.int_transaction_call_frame FINAL
    WHERE function_selector IS NOT NULL
      AND function_selector != ''
    FORMAT JSONCompact
    """

    try:
        result = execute_clickhouse_query(ch_url, query)
        selectors = set()
        if result and 'data' in result:
            for row in result['data']:
                selectors.add(row[0])
        return selectors
    except Exception as e:
        print(f"Warning: Failed to query seen selectors: {e}", file=sys.stderr)
        return set()


def get_existing_selectors(ch_url, target_db, target_table):
    """Get selectors already in dim_function_signature"""
    query = f"""
    SELECT selector
    FROM `{target_db}`.`{target_table}` FINAL
    FORMAT JSONCompact
    """

    try:
        result = execute_clickhouse_query(ch_url, query)
        selectors = set()
        if result and 'data' in result:
            for row in result['data']:
                selectors.add(row[0])
        return selectors
    except Exception as e:
        print(f"Warning: Failed to query existing selectors: {e}", file=sys.stderr)
        return set()


def lookup_signatures_batch(selectors, batch_size=20, delay=1.5, max_retries=3):
    """Lookup function signatures from Sourcify 4byte API in batches"""
    results = {}

    selector_list = list(selectors)
    total_batches = (len(selector_list) + batch_size - 1) // batch_size

    for i in range(0, len(selector_list), batch_size):
        batch = selector_list[i:i + batch_size]
        batch_num = (i // batch_size) + 1

        # Build comma-separated selector string
        selectors_param = ','.join(batch)
        url = f"https://api.4byte.sourcify.dev/signature-database/v1/lookup?function={selectors_param}&filter=true"

        # Retry logic with exponential backoff
        for attempt in range(max_retries):
            try:
                req = urllib.request.Request(url)
                req.add_header("Accept", "application/json")
                response = urllib.request.urlopen(req, timeout=30)
                data = json.loads(response.read().decode('utf-8'))

                if data.get('ok') and 'result' in data:
                    function_results = data['result'].get('function', {})

                for selector, signatures in function_results.items():
                    if signatures and len(signatures) > 0:
                        # Prefer signature with hasVerifiedContract: true
                        best_sig = None
                        for sig in signatures:
                            if sig.get('hasVerifiedContract'):
                                best_sig = sig
                                break
                        if not best_sig:
                            best_sig = signatures[0]

                        results[selector] = {
                            'name': best_sig.get('name', ''),
                            'has_verified_contract': best_sig.get('hasVerifiedContract', False)
                        }

                print(f"  Batch {batch_num}/{total_batches}: {len(batch)} selectors, {len([s for s in batch if s in results])} found")
                break  # Success, exit retry loop

            except urllib.error.HTTPError as e:
                if e.code >= 500 and attempt < max_retries - 1:
                    backoff = delay * (2 ** attempt)
                    print(f"  Batch {batch_num}/{total_batches}: HTTP {e.code}, retrying in {backoff:.1f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(backoff)
                else:
                    print(f"  Batch {batch_num}/{total_batches}: HTTP error {e.code}", file=sys.stderr)
                    break
            except Exception as e:
                if attempt < max_retries - 1:
                    backoff = delay * (2 ** attempt)
                    print(f"  Batch {batch_num}/{total_batches}: Error: {e}, retrying in {backoff:.1f}s")
                    time.sleep(backoff)
                else:
                    print(f"  Batch {batch_num}/{total_batches}: Error: {e}", file=sys.stderr)
                    break

        # Rate limiting between batches
        if i + batch_size < len(selector_list):
            time.sleep(delay)

    return results


def insert_signatures(ch_url, target_db, target_table, signatures, task_start):
    """Insert function signatures into dim_function_signature"""
    if not signatures:
        return 0

    values = []
    for selector, data in signatures.items():
        selector_escaped = selector.replace("'", "''")
        name_escaped = data['name'].replace("'", "''")
        has_verified = 'true' if data['has_verified_contract'] else 'false'

        values.append(f"""(
            fromUnixTimestamp({task_start}),
            '{selector_escaped}',
            '{name_escaped}',
            {has_verified}
        )""")

    # Batch insert
    chunk_size = 500
    inserted_count = 0
    try:
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i + chunk_size]
            insert_query = f"""
            INSERT INTO `{target_db}`.`{target_table}`
            (updated_date_time, selector, name, has_verified_contract)
            VALUES {','.join(chunk)}
            """
            execute_clickhouse_query(ch_url, insert_query)
            inserted_count += len(chunk)

        return inserted_count

    except Exception as e:
        print(f"ERROR: Failed to insert data: {e}", file=sys.stderr)
        print(f"Inserted {inserted_count} out of {len(signatures)} rows before failure", file=sys.stderr)
        raise


def main():
    ch_url = os.environ['CLICKHOUSE_URL']
    target_db = os.environ['SELF_DATABASE']
    target_table = os.environ['SELF_TABLE']
    task_start = os.environ.get('TASK_START', str(int(datetime.now().timestamp())))

    print(f"=== Function Signature Data Collection ===")
    print(f"Target: {target_db}.{target_table}")

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

        # Step 1: Get selectors we've seen in transactions
        print(f"\nFetching seen selectors from int_transaction_call_frame...")
        seen_selectors = get_seen_selectors(ch_url, target_db)
        print(f"  Found {len(seen_selectors)} distinct selectors in call frames")

        if not seen_selectors:
            print("No selectors found in int_transaction_call_frame. Nothing to do.")
            return 0

        # Step 2: Get selectors we already have
        print(f"\nFetching existing selectors from dim_function_signature...")
        existing_selectors = get_existing_selectors(ch_url, target_db, target_table)
        print(f"  Found {len(existing_selectors)} existing selectors")

        # Step 3: Compute difference
        new_selectors = seen_selectors - existing_selectors
        print(f"\nNew selectors to lookup: {len(new_selectors)}")

        if not new_selectors:
            print("No new selectors to lookup. All selectors already have signatures.")
            return 0

        # Step 4: Lookup signatures from Sourcify
        print(f"\nLooking up signatures from Sourcify 4byte API...")
        signatures = lookup_signatures_batch(new_selectors, batch_size=20, delay=1.5)
        print(f"\nFound signatures for {len(signatures)} selectors")

        if not signatures:
            print("No signatures found from Sourcify API.")
            return 0

        # Step 5: Insert signatures
        print(f"\nInserting {len(signatures)} signatures into ClickHouse...")
        inserted = insert_signatures(ch_url, target_db, target_table, signatures, task_start)
        print(f"  Successfully inserted {inserted} signatures")

        # Summary
        print(f"\nSummary:")
        print(f"  Selectors in call frames: {len(seen_selectors)}")
        print(f"  Previously known: {len(existing_selectors)}")
        print(f"  New lookups attempted: {len(new_selectors)}")
        print(f"  Signatures found: {len(signatures)}")
        print(f"  Signatures inserted: {inserted}")

        print("\nFunction signature data collection completed successfully")
        return 0

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
