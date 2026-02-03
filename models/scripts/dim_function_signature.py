#!/usr/bin/env python3
"""
Function Signature Data Collection
Collects function signature lookups from Sourcify's 4byte signature database.

This script:
1. Scans recent blocks (BLOCK_LOOKBACK) for function selectors needing lookup:
   - Selectors not yet in dim_function_signature, OR
   - Selectors with empty name (not in 4byte) older than 30 days (re-check)
2. Uses efficient LEFT JOIN instead of NOT IN for large table performance
3. Processes a batch of selectors per run (avoids OOM)
4. Batch lookups against Sourcify 4byte API
5. Inserts all looked-up selectors (empty name if not found)

The scheduled runs handle iteration until caught up. Empty names are
periodically re-checked in case 4byte adds signatures later.

Frontend note: Check `name != ''` before displaying - show fallback if empty.
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


# Maximum number of new selectors to process per run
# This prevents OOM and allows incremental catchup over multiple runs
BATCH_SIZE = 500

# Number of recent blocks to scan for new selectors
# ~50k blocks ≈ 1 week of mainnet blocks (12s block time)
# This limits the DISTINCT scan to recent data where new selectors appear
BLOCK_LOOKBACK = 50000


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


def get_new_selectors(ch_url, target_db, target_table, batch_size, block_lookback=50000):
    """
    Get selectors that need lookup from 4byte API.

    Returns selectors that either:
    1. Don't exist in dim_function_signature yet, OR
    2. Have empty name (not found previously) and are older than 30 days

    This allows re-querying unknowns periodically in case 4byte adds them later.

    Uses efficient LEFT JOIN instead of NOT IN for better performance with large tables.
    Limits scan to recent blocks (block_lookback) since new selectors come from new blocks.
    """
    query = f"""
    WITH max_block AS (
        SELECT max(block_number) as mb FROM `{target_db}`.int_transaction_call_frame
    )
    SELECT DISTINCT cf.function_selector as selector
    FROM `{target_db}`.int_transaction_call_frame cf FINAL
    LEFT JOIN (
        SELECT selector, name, updated_date_time
        FROM `{target_db}`.`{target_table}` FINAL
    ) sig ON cf.function_selector = sig.selector
    WHERE cf.block_number >= (SELECT mb - {block_lookback} FROM max_block)
      AND cf.function_selector IS NOT NULL
      AND cf.function_selector != ''
      AND (
        sig.selector IS NULL  -- Not in table yet
        OR (sig.name = '' AND sig.updated_date_time < now() - INTERVAL 30 DAY)  -- Empty & stale
      )
    LIMIT {batch_size}
    FORMAT JSONCompact
    """

    try:
        result = execute_clickhouse_query(ch_url, query)
        selectors = []
        if result and 'data' in result:
            for row in result['data']:
                selectors.append(row[0])
        return selectors
    except Exception as e:
        print(f"Error querying new selectors: {e}", file=sys.stderr)
        raise


def get_pending_count(ch_url, target_db, target_table, block_lookback=50000):
    """Get approximate count of selectors still needing lookup (for progress reporting)"""
    query = f"""
    WITH max_block AS (
        SELECT max(block_number) as mb FROM `{target_db}`.int_transaction_call_frame
    )
    SELECT count(DISTINCT cf.function_selector) as cnt
    FROM `{target_db}`.int_transaction_call_frame cf FINAL
    LEFT JOIN (
        SELECT selector, name, updated_date_time
        FROM `{target_db}`.`{target_table}` FINAL
    ) sig ON cf.function_selector = sig.selector
    WHERE cf.block_number >= (SELECT mb - {block_lookback} FROM max_block)
      AND cf.function_selector IS NOT NULL
      AND cf.function_selector != ''
      AND (
        sig.selector IS NULL
        OR (sig.name = '' AND sig.updated_date_time < now() - INTERVAL 30 DAY)
      )
    FORMAT JSONCompact
    """

    try:
        result = execute_clickhouse_query(ch_url, query)
        if result and 'data' in result and result['data']:
            return int(result['data'][0][0])
        return 0
    except Exception as e:
        print(f"Warning: Failed to get pending count: {e}", file=sys.stderr)
        return -1


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
    print(f"Batch size: {BATCH_SIZE} selectors per run")

    try:
        # Test ClickHouse connection
        print("\nTesting ClickHouse connection...")
        test_query = "SELECT 1 FORMAT JSONCompact"
        try:
            execute_clickhouse_query(ch_url, test_query)
            print("  ClickHouse connection successful")
        except Exception as e:
            print(f"ERROR: Cannot connect to ClickHouse: {e}", file=sys.stderr)
            return 1

        # Step 1: Get new selectors (computed in ClickHouse, not Python)
        print(f"\nQuerying new selectors (limit {BATCH_SIZE}, last {BLOCK_LOOKBACK} blocks)...")
        new_selectors = get_new_selectors(ch_url, target_db, target_table, BATCH_SIZE, BLOCK_LOOKBACK)
        print(f"  Found {len(new_selectors)} new selectors to lookup")

        if not new_selectors:
            print("\nNo new selectors to lookup. All selectors already have signatures.")
            return 0

        # Step 2: Get total pending count for progress reporting
        pending_count = get_pending_count(ch_url, target_db, target_table, BLOCK_LOOKBACK)
        if pending_count > 0:
            print(f"  Total pending selectors: ~{pending_count} (will process {len(new_selectors)} this run)")

        # Step 3: Lookup signatures from Sourcify
        print(f"\nLooking up signatures from Sourcify 4byte API...")
        signatures = lookup_signatures_batch(new_selectors, batch_size=20, delay=1.5)
        print(f"\nFound signatures for {len(signatures)} selectors")

        # Step 4: Insert all looked-up selectors (found with name, unknown with empty name)
        # This tracks "we looked this up" - prevents re-querying same selectors
        # Frontend should check name != '' before displaying (show fallback if empty)
        for selector in new_selectors:
            if selector not in signatures:
                signatures[selector] = {
                    'name': '',
                    'has_verified_contract': False
                }

        found_count = len([s for s in signatures.values() if s['name']])
        not_found_count = len(signatures) - found_count

        print(f"\nInserting {len(signatures)} entries into ClickHouse...")
        inserted = insert_signatures(ch_url, target_db, target_table, signatures, task_start)
        print(f"  Successfully inserted {inserted} entries")

        print(f"\nSummary:")
        print(f"  Selectors processed: {len(new_selectors)}")
        print(f"  With signature: {found_count}")
        print(f"  Not in 4byte (empty name): {not_found_count}")
        print(f"  Total inserted: {inserted}")
        remaining = pending_count - len(new_selectors) if pending_count > 0 else 0
        if remaining > 0:
            print(f"  Remaining to process: ~{remaining} (will continue in next scheduled run)")

        print("\nFunction signature data collection completed successfully")
        return 0

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
