"""
MySQL/Doris Schema Extraction Script
Connects to MySQL/Doris database and generates M-Schema.json files with table grouping and LLM-based naming rule analysis.
"""
import os
import sys
import json
import random
import argparse
import re
import time
from pathlib import Path
from collections import defaultdict

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

import pymysql
from pymysql import Error as PyMySQLError
from utils.DBsetup.Get_DB import read_db_config
from LLM.LLM_OUT import LLM_output
from utils.extract_json import extract_and_parse_json

# Global Configuration
OVERWRITE_EXISTING_JSON = False
INCLUDE_COLUMN_DESCRIPTIONS = True
EXAMPLE_LIMIT = 3  # Number of example values to retrieve per column

# LLM Prompt for table naming rule analysis (similar to BigQuery/Snowflake)
TABLE_NAMING_PROMPT = '''
I have a set of database tables with the same structure but varying names. Please:  
1. Induce the naming rules of these tables, marking the variable parts with descriptive placeholders (e.g., `[REGION]`, `[VERSION]`, `[TYPE]`, `[SHARD_ID]`, etc.).  
2. Summarize the meaning or representation of these tables in a short paragraph.  
3. For each placeholder, explain its meaning and all value ranges appearing in this set of table names (e.g., "\[SHARD_ID] ranges from 0 to 15, representing hash-based sharding").

An example is as follows:
【Table_Name】
"user_table_0": [
"user_table_1",
"user_table_2",
"user_table_3",
...
"user_table_15"
]
【Answer】
The naming convention for these tables with the same structure is:
`user_table_[SHARD_ID]`, where:  
- `[SHARD_ID]` denotes the shard identifier, ranging from 0 to 15, representing hash-based sharding of user data across 16 shards.
These tables represent user data distributed across multiple shards for horizontal scaling and performance optimization.

Now, please handle the following problem:
【Table_Name】
{Table_Name}
【Column_Description】
{Column_Description}
【Extra_info】
{extra_cols_info}

## Output format(Markdown)
<Analysis Summary>Analysis Summary put there</Analysis Summary>
**return**
```json
{{
"Answer":"Please provide the answer here (refer to the example)!",
}}
```
'''

def get_table_names(conn, database_name):
    """Get all user-defined table names from the database."""
    cursor = conn.cursor()
    query = f"""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """
    cursor.execute(query, (database_name,))
    table_names = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return table_names

def get_table_schema(conn, database_name, table_name):
    """Get column information for a specific table."""
    cursor = conn.cursor()
    query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            COLUMN_TYPE,
            IS_NULLABLE,
            COLUMN_KEY,
            COLUMN_DEFAULT,
            EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query, (database_name, table_name))
    columns = []
    for row in cursor.fetchall():
        col_name, data_type, col_type, is_nullable, column_key, col_default, extra = row
        is_pk = (column_key == 'PRI')
        # Normalize data type
        normalized_type = normalize_mysql_type(data_type, col_type)
        columns.append({
            'name': col_name,
            'type': normalized_type,
            'is_pk': is_pk,
            'is_nullable': is_nullable == 'YES',
            'default': col_default
        })
    cursor.close()
    return columns

def normalize_mysql_type(data_type, col_type):
    """Normalize MySQL data types to standard format."""
    data_type_upper = data_type.upper()
    
    # Map MySQL types to standard types
    type_mapping = {
        'TINYINT': 'NUMBER',
        'SMALLINT': 'NUMBER',
        'MEDIUMINT': 'NUMBER',
        'INT': 'NUMBER',
        'INTEGER': 'NUMBER',
        'BIGINT': 'NUMBER',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'FLOAT',
        'DECIMAL': 'FLOAT',
        'NUMERIC': 'FLOAT',
        'CHAR': 'TEXT',
        'VARCHAR': 'TEXT',
        'TEXT': 'TEXT',
        'TINYTEXT': 'TEXT',
        'MEDIUMTEXT': 'TEXT',
        'LONGTEXT': 'TEXT',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'DATETIME': 'DATE',
        'TIMESTAMP': 'DATE',
        'YEAR': 'NUMBER',
        'BINARY': 'TEXT',
        'VARBINARY': 'TEXT',
        'BLOB': 'TEXT',
        'TINYBLOB': 'TEXT',
        'MEDIUMBLOB': 'TEXT',
        'LONGBLOB': 'TEXT',
        'JSON': 'TEXT',
        'ENUM': 'TEXT',
        'SET': 'TEXT'
    }
    
    return type_mapping.get(data_type_upper, 'TEXT')

def get_foreign_keys(conn, database_name, table_names):
    """Get foreign key relationships."""
    cursor = conn.cursor()
    foreign_keys = {}
    
    for table_name in table_names:
        query = f"""
            SELECT 
                COLUMN_NAME,
                REFERENCED_TABLE_SCHEMA,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_NAME = %s
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        cursor.execute(query, (database_name, table_name))
        for row in cursor.fetchall():
            col_name, ref_schema, ref_table, ref_col = row
            if ref_table in table_names:  # Only include if referenced table is in our list
                source_key = f"{table_name}.{col_name}"
                target_key = f"{ref_table}.{ref_col}"
                foreign_keys[source_key] = target_key
    
    cursor.close()
    return foreign_keys

def get_column_examples(conn, database_name, table_name, column_name, limit=EXAMPLE_LIMIT):
    """Get example values from a column."""
    cursor = conn.cursor()
    try:
        # Use DISTINCT to get unique values, and random sampling
        query = f"""
            SELECT DISTINCT `{column_name}` 
            FROM `{database_name}`.`{table_name}`
            WHERE `{column_name}` IS NOT NULL
            ORDER BY RAND()
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        examples = [str(row[0]) for row in cursor.fetchall() if row[0] is not None]
        return examples[:limit]
    except Exception as e:
        print(f"  ⚠️  Warning: Could not get examples for {table_name}.{column_name}: {e}")
        return []
    finally:
        cursor.close()

def remove_digits(s):
    """Remove digits from string to identify table series."""
    if not isinstance(s, str):
        s = str(s)
    return re.sub(r'\d', '', s)

def group_tables_by_series(table_names):
    """Group tables with similar structure by removing digits from names."""
    groups = defaultdict(list)
    for table_name in table_names:
        base_name = remove_digits(table_name)
        groups[base_name].append(table_name)
    return groups

def _generate_table_group_description(table_list, base_rep_table, rep_table_structure, extra_cols_info, llm_params):
    """Generate description for a group of similar tables using LLM."""
    similar_tables = [t for t in table_list if t != base_rep_table]
    description = ""
    
    if len(table_list) <= 5:
        # For small groups, use simple template
        similar_tables_str = ', '.join(f"'{t}'" for t in similar_tables)
        description = f"Tables {similar_tables_str} and the current table '{base_rep_table}' share a similar column pattern."
        
        if extra_cols_info:
            cols_to_tables = defaultdict(list)
            for tname, cols in extra_cols_info.items():
                if cols:
                    extra_col_name = cols[0]['column_name']
                    cols_to_tables[extra_col_name].append(tname)
            
            group_notes = []
            for col_name, tnames in cols_to_tables.items():
                quoted_tnames = [f"'{t}'" for t in tnames]
                if len(quoted_tnames) == 1:
                    tables_str = quoted_tnames[0]
                    group_notes.append(f"table {tables_str} has an extra column '{col_name}'")
                else:
                    tables_str = f"{', '.join(quoted_tnames[:-1])} and {quoted_tnames[-1]}"
                    group_notes.append(f"tables {tables_str} have a common extra column '{col_name}'")
            
            if group_notes:
                extra_notes = " Additionally: " + "; ".join(group_notes) + "."
                description += extra_notes
    else:
        # For large groups, use LLM to analyze naming rules
        Table_Name_dict = {base_rep_table: similar_tables}
        column_descriptions = [col.get('description') for col in rep_table_structure if col.get('description')]
        Column_Description = str(set(filter(None, column_descriptions)))
        
        extra_info_for_prompt = ""
        if extra_cols_info:
            extra_info_for_prompt = json.dumps(dict(extra_cols_info), indent=2)
        
        attempt = 0
        while attempt < llm_params.get('max_retries', 5):
            try:
                formatted_prompt = TABLE_NAMING_PROMPT.format(
                    Table_Name=json.dumps(Table_Name_dict, indent=2),
                    Column_Description=Column_Description,
                    extra_cols_info=extra_info_for_prompt
                )
                messages = [{"role": "user", "content": formatted_prompt}]
                print(f"--- Calling LLM for table group starting with '{base_rep_table}' ---")
                _, _, Thinking, LLM_return = LLM_output(
                    messages=messages,
                    model=llm_params.get('model', 'deepseek-chat'),
                    temperature=llm_params.get('temperature', 0)
                )
                
                temp = extract_and_parse_json(LLM_return)
                if not temp or "Answer" not in temp:
                    raise ValueError("LLM returned empty or invalid JSON.")
                description = temp["Answer"]
                print("--- LLM call successful ---")
                break
            except Exception as e:
                attempt += 1
                print(f"⚠️ LLM call failed (attempt {attempt}), retrying... Error: {e}")
                if attempt >= llm_params.get('max_retries', 5):
                    similar_tables_str = ', '.join(f"'{t}'" for t in similar_tables)
                    description = f"Tables {similar_tables_str} and the current table '{base_rep_table}' share a similar column pattern."
                    print("❌ Reached maximum retry attempts, using default description.")
                    break
                time.sleep(llm_params.get('retry_delay', 2))
    
    return description

def process_table_series(table_names, conn, database_name, db_name, model="deepseek-chat"):
    """Process a series of tables with similar structure, group them and generate description."""
    # Group tables by series
    table_groups = group_tables_by_series(table_names)
    
    db_content = {}
    table_info = {}
    table_description_summary = {}
    
    llm_params = {
        'max_retries': 5,
        'retry_delay': 2,
        'model': model,
        'temperature': 0
    }
    
    for base_name, table_list in table_groups.items():
        if len(table_list) == 1:
            # Single table, process normally
            table_name = table_list[0]
            print(f"  📋 Processing single table: {table_name}")
            table_schema = get_table_schema(conn, database_name, table_name)
            
            columns_json = []
            for col_info in table_schema:
                col_name = col_info['name']
                examples = get_column_examples(conn, database_name, table_name, col_name, EXAMPLE_LIMIT)
                examples_str = ", ".join(map(str, examples)) if examples else ""
                
                column_entry = [
                    col_name,
                    "Primary Key" if col_info['is_pk'] else None,
                    col_info['type'],
                    None,
                    examples_str
                ]
                columns_json.append(column_entry)
            
            db_content[table_name] = columns_json
            print(f"    ✅ Processed {len(columns_json)} columns")
        else:
            # Multiple tables with similar structure, process as series
            table_list.sort()
            rep_table = table_list[0]  # Use first table as representative
            
            print(f"  📋 Processing table series: {len(table_list)} tables (representative: {rep_table})")
            
            # Get schema for representative table
            rep_schema = get_table_schema(conn, database_name, rep_table)
            rep_cols_set = {col['name'].lower() for col in rep_schema}
            
            # Check for extra columns in other tables
            extra_cols_info = defaultdict(list)
            for table_name in table_list[1:]:
                current_schema = get_table_schema(conn, database_name, table_name)
                current_cols_set = {col['name'].lower() for col in current_schema}
                extra_cols = current_cols_set - rep_cols_set
                if extra_cols:
                    extra_col_name = next(iter(extra_cols))
                    extra_cols_info[table_name].append({"column_name": extra_col_name})
            
            # Format representative table structure for LLM
            rep_table_structure = [
                {'column_name': col['name'], 'description': None, 'column_type': col['type']}
                for col in rep_schema
            ]
            
            # Generate description using LLM
            description = _generate_table_group_description(
                table_list, rep_table, rep_table_structure, extra_cols_info, llm_params
            )
            
            # Process representative table columns
            columns_json = []
            for col_info in rep_schema:
                col_name = col_info['name']
                # Aggregate examples from all tables in series
                all_examples = []
                for table_name in table_list[:3]:  # Sample from first 3 tables
                    examples = get_column_examples(conn, database_name, table_name, col_name, 1)
                    all_examples.extend(examples)
                    if len(all_examples) >= EXAMPLE_LIMIT:
                        break
                examples_str = ", ".join(map(str, all_examples[:EXAMPLE_LIMIT])) if all_examples else ""
                
                column_entry = [
                    col_name,
                    "Primary Key" if col_info['is_pk'] else None,
                    col_info['type'],
                    None,
                    examples_str
                ]
                columns_json.append(column_entry)
            
            db_content[rep_table] = columns_json
            table_info[rep_table] = [t for t in table_list if t != rep_table]
            table_description_summary[rep_table] = description
            print(f"    ✅ Processed series with {len(columns_json)} columns")
    
    return db_content, table_info, table_description_summary

def process_database_mysql(db_name, credentials, db_root, overwrite_existing=False, include_descriptions=True, model="deepseek-chat"):
    """
    Process a MySQL/Doris database, extract schema information with table grouping and LLM analysis, and generate a JSON file.
    """
    output_dir = os.path.join(db_root, db_name)
    output_json_path = os.path.join(output_dir, f"{db_name}_M-Schema.json")
    
    if not overwrite_existing and os.path.exists(output_json_path):
        print(f"File '{os.path.basename(output_json_path)}' already exists. Skipping database '{db_name}'.")
        return
    
    # Get database name from credentials if not provided
    database_name = credentials.get("database", db_name)
    
    print(f"Processing database: {db_name} (connecting to: {database_name})...")
    
    conn = None
    try:
        # Connect to MySQL/Doris
        conn = pymysql.connect(
            host=credentials.get("host", "localhost"),
            port=credentials.get("port", 3306),
            user=credentials.get("user"),
            password=credentials.get("password"),
            database=database_name,
            charset='utf8mb4'
        )
        print("✅ Connection successful!")
        
        # Get table names
        table_names = get_table_names(conn, database_name)
        print(f"  Found {len(table_names)} tables.")
        
        if not table_names:
            print(f"  ⚠️  No tables found in database '{database_name}'. Skipping.")
            return
        
        # Get foreign keys
        foreign_keys = get_foreign_keys(conn, database_name, table_names)
        print(f"  Found {len(foreign_keys)} foreign key relationships.")
        
        # Process tables with grouping and LLM analysis
        db_content, table_info, table_description_summary = process_table_series(
            table_names, conn, database_name, db_name, model
        )
        
        # Create final JSON structure (compatible with BigQuery/Snowflake format)
        final_json_data = {
            db_name: db_content
        }
        
        if foreign_keys:
            final_json_data["foreign_keys"] = foreign_keys
        
        if table_info:
            final_json_data["table_Information"] = table_info
        
        if table_description_summary:
            final_json_data["table_description_summary"] = table_description_summary
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save JSON file
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(final_json_data, f, indent=4, ensure_ascii=False)
        
        print(f"✅ Successfully generated Schema JSON file: {output_json_path}")
        
    except PyMySQLError as e:
        print(f"❌ MySQL/Doris error occurred while processing database '{db_name}': {e}")
    except Exception as e:
        print(f"❌ Error occurred while processing database '{db_name}': {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

def main():
    """Main function to process MySQL/Doris databases."""
    parser = argparse.ArgumentParser(description='Extract MySQL/Doris database schemas')
    parser.add_argument('--db_type', type=str, choices=['mysql', 'doris'], default='mysql',
                        help='Database type: mysql or doris')
    parser.add_argument('--db_name', type=str, default=None,
                        help='Specific database name to process (if not provided, processes all in config)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing JSON files')
    parser.add_argument('--model', type=str, default='deepseek-chat',
                        help='LLM model name for table naming rule analysis')
    
    args = parser.parse_args()
    
    # Read database configuration
    sqlite_path, snow_path, bigquery_path, mysql_path, doris_path, snow_auth, bigquery_auth, mysql_auth, doris_auth = read_db_config()
    
    # Select appropriate paths and credentials
    if args.db_type == 'mysql':
        db_root = mysql_path if mysql_path else "spider2-lite/resource/databases/mysql"
        credentials_path = mysql_auth
    else:  # doris
        db_root = doris_path if doris_path else "spider2-lite/resource/databases/doris"
        credentials_path = doris_auth
    
    # Load credentials
    if not credentials_path or not os.path.exists(credentials_path):
        print(f"❌ Error: Credentials file not found: {credentials_path}")
        print(f"   Please create the credentials file with MySQL/Doris connection information.")
        return
    
    with open(credentials_path, 'r', encoding='utf-8') as f:
        credentials = json.load(f)
    
    print(f"🔌 Using {args.db_type.upper()} credentials from: {credentials_path}")
    print(f"📁 Output directory: {db_root}")
    print(f"🤖 Using LLM model: {args.model}")
    
    # Process database(s)
    if args.db_name:
        # Process specific database
        process_database_mysql(
            db_name=args.db_name,
            credentials=credentials,
            db_root=db_root,
            overwrite_existing=args.overwrite,
            include_descriptions=INCLUDE_COLUMN_DESCRIPTIONS,
            model=args.model
        )
    else:
        # Process all databases listed in a directory or from config
        # For now, use the database name from credentials
        db_name = credentials.get("database", "default_db")
        process_database_mysql(
            db_name=db_name,
            credentials=credentials,
            db_root=db_root,
            overwrite_existing=args.overwrite,
            include_descriptions=INCLUDE_COLUMN_DESCRIPTIONS,
            model=args.model
        )
        print(f"\n💡 Tip: To process a specific database, use --db_name option")

if __name__ == '__main__':
    main()

