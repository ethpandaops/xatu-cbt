#!/usr/bin/env python3
"""
Entity Data Collection for Validators
Collects validator entity data from ethpandaops inventories or ethseer

This script:
1. Downloads networks data from cartographoor
2. Matches database to network configuration
3. For devnet networks: parses inventory.ini for validator data
4. For other networks: uses ethseer_validator_entity table if available
5. Inserts data into int_entity table
"""

import os
import sys
import urllib.request
import urllib.parse
import json
import re
from datetime import datetime
from configparser import ConfigParser
from io import StringIO

def execute_clickhouse_query(url, query):
    """Execute a query via ClickHouse HTTP interface"""
    try:
        req = urllib.request.Request(url, 
                                    data=query.encode('utf-8'),
                                    method='POST')
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
    """Fetch content from a URL"""
    try:
        req = urllib.request.Request(url)
        response = urllib.request.urlopen(req)
        return response.read().decode('utf-8')
    except Exception as e:
        print(f"Failed to fetch {url}: {e}", file=sys.stderr)
        return None

def parse_inventory_for_validators(inventory_content):
    """Parse ansible inventory.ini for validator data"""
    validators = []
    
    # Clean up inventory content - remove lines before first section
    lines = inventory_content.split('\n')
    cleaned_lines = []
    in_section = False
    for line in lines:
        if line.strip().startswith('['):
            in_section = True
        if in_section or not line.strip():
            cleaned_lines.append(line)
    
    cleaned_content = '\n'.join(cleaned_lines)
    
    # Parse the ini file
    config = ConfigParser(allow_no_value=True)
    config.read_string(cleaned_content)
    
    # Process each section (group)
    for section in config.sections():
        if section == 'all:vars':
            continue
            
        entity = section  # Group name is the entity
        
        # Process each host in the group
        for host in config.options(section):
            if host.startswith(';') or host.startswith('#'):
                continue
                
            # Get the host line value
            host_line = config.get(section, host)
            if not host_line:
                continue
            
            # Parse validator_start and validator_end from the host line
            validator_start_match = re.search(r'validator_start=(\d+)', host_line)
            validator_end_match = re.search(r'validator_end=(\d+)', host_line)
            
            if validator_start_match and validator_end_match:
                validator_start = int(validator_start_match.group(1))
                validator_end = int(validator_end_match.group(1))
                
                # Parse cloud_region
                region_match = re.search(r'cloud_region=(\w+)', host_line)
                region = region_match.group(1) if region_match else None
                
                # Parse clients from the host name
                # Format: consensus-execution-type-number
                name_parts = host.split('-')
                consensus_client = name_parts[0] if len(name_parts) > 0 else None
                execution_client = name_parts[1] if len(name_parts) > 1 else None
                
                # Generate a row for each validator index
                for validator_index in range(validator_start, validator_end):
                    validators.append({
                        'validator_index': validator_index,
                        'entity': entity,
                        'name': host,
                        'classification': 'ethpandaops',
                        'region': region,
                        'consensus_client': consensus_client,
                        'execution_client': execution_client,
                        'validator_start': validator_start,
                        'validator_end': validator_end
                    })
    
    return validators

def check_table_exists(ch_url, database, table):
    """Check if a table exists in ClickHouse"""
    query = f"""
    SELECT count(*) as cnt
    FROM system.tables
    WHERE database = '{database}' AND name = '{table}'
    FORMAT JSONCompact
    """
    result = execute_clickhouse_query(ch_url, query)
    if 'data' in result and result['data']:
        return int(result['data'][0][0]) > 0
    return False

def main():
    # Get environment variables
    ch_url = os.environ['CLICKHOUSE_URL']
    
    # Model info
    target_db = os.environ['SELF_DATABASE']
    target_table = os.environ['SELF_TABLE']
    
    # Task context - use current time for updated_date_time
    task_start = int(os.environ.get('TASK_START', int(datetime.now().timestamp())))
    
    print(f"=== Entity Data Collection ===")
    print(f"Target: {target_db}.{target_table}")
    print(f"Database: {target_db}")
    print(f"ClickHouse URL: {ch_url}")
    
    try:
        # Step 1: Download networks data
        networks_url = "https://ethpandaops-platform-production-cartographoor.ams3.digitaloceanspaces.com/networks.json"
        print(f"Fetching networks data from {networks_url}")
        networks_json = fetch_url(networks_url)
        
        if not networks_json:
            print("Failed to fetch networks data", file=sys.stderr)
            return 1
        
        networks_data = json.loads(networks_json)
        networks = networks_data.get('networks', {})
        
        # Step 2: Match database to network
        database_name = target_db
        network_info = networks.get(database_name, {})
        
        print(f"Network info for {database_name}: {network_info}")
        
        validators_to_insert = []
        
        # Step 3: Check if this is an ethpandaops network with repository
        if network_info.get('repository'):
            repository = network_info['repository']
            network_name = network_info.get('name', database_name)
            
            # Construct inventory URL
            inventory_url = f"https://raw.githubusercontent.com/{repository}/refs/heads/master/ansible/inventories/{network_name}/inventory.ini"
            print(f"Fetching inventory from {inventory_url}")
            
            inventory_content = fetch_url(inventory_url)
            
            if inventory_content:
                # Parse inventory for validators
                validators_to_insert = parse_inventory_for_validators(inventory_content)
                print(f"Found {len(validators_to_insert)} validator entries from ethpandaops inventory")
            else:
                print(f"Failed to fetch inventory for {database_name}")
        
        # Step 4: If no ethpandaops data, check for ethseer table
        if not validators_to_insert:
            ethseer_table = 'ethseer_validator_entity'
            print(f"Checking for {database_name}.{ethseer_table} table")
            
            if check_table_exists(ch_url, database_name, ethseer_table):
                print(f"Found {database_name}.{ethseer_table}, fetching entity data")
                
                # Query ethseer data
                query = f"""
                SELECT DISTINCT
                    index as validator_index,
                    entity
                FROM `{database_name}`.`{ethseer_table}`
                WHERE entity != ''
                ORDER BY index
                FORMAT JSONCompact
                """
                
                result = execute_clickhouse_query(ch_url, query)
                
                if 'data' in result and result['data']:
                    for row in result['data']:
                        validators_to_insert.append({
                            'validator_index': int(row[0]),
                            'entity': row[1],
                            'name': None,
                            'classification': 'unclassified',
                            'region': None,
                            'consensus_client': None,
                            'execution_client': None
                        })
                    print(f"Found {len(validators_to_insert)} validator entries from ethseer")
            else:
                print(f"No {database_name}.{ethseer_table} table found")
        
        # Step 5: Insert data into int_entity table
        if validators_to_insert:
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{target_db}`.`{target_table}` (
                updated_date_time DateTime,
                validator_index UInt32,
                entity String,
                name Nullable(String),
                classification LowCardinality(String),
                region Nullable(String),
                consensus_client Nullable(String),
                execution_client Nullable(String)
            ) ENGINE = ReplacingMergeTree(updated_date_time)
            ORDER BY (validator_index)
            """
            execute_clickhouse_query(ch_url, create_table_query)
            print(f"✓ Ensured target table exists")
            
            # Prepare batch insert
            values = []
            for validator in validators_to_insert:
                # Escape strings properly
                entity_escaped = validator['entity'].replace("'", "''")
                name_escaped = "'" + validator['name'].replace("'", "''") + "'" if validator['name'] else 'NULL'
                region_escaped = "'" + validator['region'].replace("'", "''") + "'" if validator['region'] else 'NULL'
                consensus_escaped = "'" + validator['consensus_client'].replace("'", "''") + "'" if validator['consensus_client'] else 'NULL'
                execution_escaped = "'" + validator['execution_client'].replace("'", "''") + "'" if validator['execution_client'] else 'NULL'
                
                values.append(f"""
                    (now(), {validator['validator_index']}, '{entity_escaped}', 
                     {name_escaped}, '{validator['classification']}', {region_escaped},
                     {consensus_escaped}, {execution_escaped})
                """)
            
            # Batch insert (in chunks to avoid too large queries)
            chunk_size = 1000
            for i in range(0, len(values), chunk_size):
                chunk = values[i:i+chunk_size]
                insert_query = f"""
                INSERT INTO `{target_db}`.`{target_table}` 
                (updated_date_time, validator_index, entity, name, 
                 classification, region, consensus_client, execution_client)
                VALUES {','.join(chunk)}
                """
                execute_clickhouse_query(ch_url, insert_query)
            
            print(f"✓ Inserted {len(validators_to_insert)} validator entities")
            
            # Summary statistics
            entity_counts = {}
            for v in validators_to_insert:
                entity = v['entity']
                entity_counts[entity] = entity_counts.get(entity, 0) + 1
            
            print(f"\nEntity Summary:")
            print(f"  Total entities: {len(entity_counts)}")
            print(f"  Total validators: {len(validators_to_insert)}")
            
            # Show top entities
            top_entities = sorted(entity_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            print(f"\n  Top 5 entities by validator count:")
            for entity, count in top_entities:
                print(f"    - {entity}: {count} validators")
        else:
            print("No validator entity data found for this network")
        
        print("\n✅ Entity data collection completed successfully")
        return 0
        
    except Exception as e:
        print(f"\n❌ Error collecting entity data: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())