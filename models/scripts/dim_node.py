#!/usr/bin/env python3
"""
Node Data Collection for Validators
Collects validator node data from ethpandaops cartographer and ethseer

This script:
1. Downloads validator ranges data from cartographer for the network
2. Queries ethseer_validator_entity table for additional validator mappings
3. Expands validator ranges into individual validator rows
4. Merges data from both sources, with cartographer taking precedence
5. Inserts combined data into dim_node table
"""

import os
import sys
import urllib.request
import urllib.error
import urllib.parse
import json
import base64
from datetime import datetime

def execute_clickhouse_query(url, query):
    """Execute a query via ClickHouse HTTP interface with proper auth handling"""
    try:
        # Parse URL to extract auth and build clean URL
        parsed = urllib.parse.urlparse(url)
        
        # Build URL without auth credentials
        if parsed.port:
            clean_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}{parsed.path}"
        else:
            clean_url = f"{parsed.scheme}://{parsed.hostname}{parsed.path}"
        
        if parsed.query:
            clean_url += f"?{parsed.query}"
        
        req = urllib.request.Request(clean_url, 
                                    data=query.encode('utf-8'),
                                    method='POST')
        
        # Add Basic Auth header if credentials exist
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

def fetch_url(url):
    """Fetch content from a URL with proper error handling"""
    try:
        req = urllib.request.Request(url)
        response = urllib.request.urlopen(req)
        return response.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"URL not found (404): {url}", file=sys.stderr)
            raise
        else:
            print(f"HTTP error {e.code} fetching {url}: {e}", file=sys.stderr)
            raise
    except Exception as e:
        print(f"Failed to fetch {url}: {e}", file=sys.stderr)
        raise

def parse_validator_ranges_data(json_data, database_name):
    """Parse validator ranges JSON and expand into individual validator rows"""
    validators = {}
    
    data = json.loads(json_data)
    nodes = data.get('nodes', {})
    
    for node_name, node_info in nodes.items():
        groups = node_info.get('groups', [])
        tags = node_info.get('tags', [])
        attributes = node_info.get('attributes', {})
        source = node_info.get('source', 'unknown')
        validator_ranges = node_info.get('validatorRanges', [])
        
        # Expand validator ranges into individual rows
        for vrange in validator_ranges:
            start = vrange.get('start', 0)
            end = vrange.get('end', 0)
            
            # Create a row for each validator index in the range (inclusive)
            for validator_index in range(start, end + 1):
                validators[validator_index] = {
                    'name': node_name,
                    'groups': groups,
                    'tags': tags + ['source:cartographer'],
                    'attributes': attributes,
                    'validator_index': validator_index,
                    'source': source
                }
    
    return validators

def fetch_ethseer_validators(ch_url, database_name):
    """Fetch validator data from ethseer_validator_entity table"""
    query = f"""
    SELECT 
        `index` AS validator_index,
        entity AS source
    FROM `{database_name}`.ethseer_validator_entity
    FINAL
    FORMAT JSONCompact
    """
    
    try:
        result = execute_clickhouse_query(ch_url, query)
        validators = {}
        
        if result and 'data' in result:
            for row in result['data']:
                validator_index = int(row[0])
                source = row[1]
                
                validators[validator_index] = {
                    'name': None,  # No name from ethseer
                    'groups': [],
                    'tags': ['source:ethseer'],
                    'attributes': {},
                    'validator_index': validator_index,
                    'source': source
                }
        
        return validators
    except Exception as e:
        print(f"Warning: Failed to fetch ethseer validators: {e}", file=sys.stderr)
        return {}

def merge_validator_data(cartographer_validators, ethseer_validators):
    """Merge validator data from both sources, with cartographer taking precedence"""
    # Start with all cartographer validators (they have richer metadata)
    merged = dict(cartographer_validators)
    
    # Add ethseer validators that are not in cartographer
    ethseer_only_count = 0
    for validator_index, ethseer_data in ethseer_validators.items():
        if validator_index not in merged:
            merged[validator_index] = ethseer_data
            ethseer_only_count += 1
    
    print(f"  Validators from cartographer: {len(cartographer_validators)}")
    print(f"  Validators from ethseer only (gap-filled): {ethseer_only_count}")
    print(f"  Total unique validators: {len(merged)}")
    
    return merged

def main():
    # Get environment variables
    ch_url = os.environ['CLICKHOUSE_URL']
    
    # Model info
    target_db = os.environ['SELF_DATABASE']
    target_table = os.environ['SELF_TABLE']
    
    print(f"=== Node Data Collection ===")
    print(f"Target: {target_db}.{target_table}")
    print(f"Database: {target_db}")
    print(f"ClickHouse URL: {ch_url}")
    
    try:
        # Test ClickHouse connection first
        print("Testing ClickHouse connection...")
        test_query = "SELECT 1 FORMAT JSONCompact"
        try:
            execute_clickhouse_query(ch_url, test_query)
            print("✓ ClickHouse connection successful")
        except Exception as e:
            print(f"ERROR: Cannot connect to ClickHouse at {ch_url}", file=sys.stderr)
            print(f"Connection error: {e}", file=sys.stderr)
            print("Please check the CLICKHOUSE_URL environment variable", file=sys.stderr)
            return 1
        # Step 1: Construct URL for validator ranges data
        # Use the database name as the network name for the JSON file
        validator_ranges_url = f"https://ethpandaops-platform-production-cartographoor.ams3.cdn.digitaloceanspaces.com/validator-ranges/{target_db}.json"
        print(f"Fetching validator ranges data from {validator_ranges_url}")
        
        # Step 2: Fetch the data
        try:
            json_content = fetch_url(validator_ranges_url)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"ERROR: Validator ranges data not found for network '{target_db}'", file=sys.stderr)
                print(f"The URL {validator_ranges_url} returned 404", file=sys.stderr)
                return 1
            raise
        
        # Step 3: Parse the cartographer data
        cartographer_validators = parse_validator_ranges_data(json_content, target_db)
        print(f"Found {len(cartographer_validators)} validator entries from cartographer")
        
        # Step 4: Fetch ethseer validators
        print(f"\nFetching validators from ethseer_validator_entity table...")
        ethseer_validators = fetch_ethseer_validators(ch_url, target_db)
        print(f"Found {len(ethseer_validators)} validator entries from ethseer")
        
        # Step 5: Merge data from both sources
        print(f"\nMerging validator data from both sources...")
        merged_validators = merge_validator_data(cartographer_validators, ethseer_validators)
        validators_to_insert = list(merged_validators.values())
        
        if not validators_to_insert:
            print("No validator data found from either source")
            return 0
        
        # Show summary of what we found
        node_counts = {}
        unique_groups = set()
        unique_tags = set()
        
        for v in validators_to_insert:
            node_name = v['name']
            if node_name:
                node_counts[node_name] = node_counts.get(node_name, 0) + 1
            unique_groups.update(v['groups'])
            unique_tags.update(v['tags'])
        
        print(f"\nData Summary (before insertion):")
        print(f"  Total nodes: {len(node_counts)}")
        print(f"  Total validators: {len(validators_to_insert)}")
        print(f"  Unique groups: {len(unique_groups)}")
        print(f"  Unique tags: {len(unique_tags)}")
        
        # Show top nodes by validator count
        if node_counts:
            top_nodes = sorted(node_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            print(f"\n  Top 5 nodes by validator count:")
            for node_name, count in top_nodes:
                print(f"    - {node_name}: {count} validators")
        
        # Step 6: Create table if it doesn't exist
        print(f"\nPreparing to insert data into ClickHouse...")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{target_db}`.`{target_table}` (
            updated_date_time DateTime,
            name String,
            groups Array(String),
            tags Array(String),
            attributes Map(String, String),
            validator_index UInt32,
            source String
        ) ENGINE = ReplacingMergeTree(updated_date_time)
        ORDER BY (validator_index, name)
        """
        
        try:
            execute_clickhouse_query(ch_url, create_table_query)
            print(f"✓ Ensured target table exists")
        except Exception as e:
            print(f"ERROR: Failed to create/verify table in ClickHouse", file=sys.stderr)
            print(f"Error: {e}", file=sys.stderr)
            print(f"\nNote: Successfully fetched {len(validators_to_insert)} validator entries but cannot insert them", file=sys.stderr)
            return 1
        
        # Step 7: Prepare and insert data
        values = []
        for validator in validators_to_insert:
            # Escape strings properly
            name_escaped = validator['name'].replace("'", "''") if validator['name'] else 'NULL'
            source_escaped = validator['source'].replace("'", "''")
            
            # Format arrays
            groups_array = "[" + ",".join(["'" + g.replace("'", "''") + "'" for g in validator['groups']]) + "]"
            tags_array = "[" + ",".join(["'" + t.replace("'", "''") + "'" for t in validator['tags']]) + "]"
            
            # Format map - handle various value types
            map_items = []
            for k, v in validator['attributes'].items():
                k_escaped = k.replace("'", "''")
                # Convert boolean and other types to string
                if isinstance(v, bool):
                    v_str = 'true' if v else 'false'
                elif v is None:
                    v_str = 'null'
                else:
                    v_str = str(v)
                v_escaped = v_str.replace("'", "''")
                map_items.append(f"'{k_escaped}':'{v_escaped}'")
            attributes_map = "{" + ",".join(map_items) + "}"
            
            # Handle NULL name for ethseer-only validators
            if validator['name'] is None:
                values.append(f"""
                    (now(), NULL, {groups_array}, {tags_array}, 
                     {attributes_map}, {validator['validator_index']}, '{source_escaped}')
                """)
            else:
                values.append(f"""
                    (now(), '{name_escaped}', {groups_array}, {tags_array}, 
                     {attributes_map}, {validator['validator_index']}, '{source_escaped}')
                """)
        
        # Batch insert (in chunks to avoid too large queries)
        chunk_size = 1000
        inserted_count = 0
        try:
            for i in range(0, len(values), chunk_size):
                chunk = values[i:i+chunk_size]
                insert_query = f"""
                INSERT INTO `{target_db}`.`{target_table}` 
                (updated_date_time, name, groups, tags, attributes, validator_index, source)
                VALUES {','.join(chunk)}
                """
                execute_clickhouse_query(ch_url, insert_query)
                inserted_count += len(chunk)
                if i % 10000 == 0 and i > 0:
                    print(f"  Progress: {inserted_count}/{len(validators_to_insert)} rows inserted...")
            
            print(f"✓ Successfully inserted {inserted_count} validator node entries")
            print("\n✅ Node data collection completed successfully")
        except Exception as e:
            print(f"ERROR: Failed to insert data into ClickHouse", file=sys.stderr)
            print(f"Error: {e}", file=sys.stderr)
            print(f"Inserted {inserted_count} out of {len(validators_to_insert)} rows before failure", file=sys.stderr)
            return 1
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error collecting node data: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())