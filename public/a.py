import snowflake.connector
import random

# Snowflake connection details
SNOWFLAKE_USER = 'your_username'
SNOWFLAKE_PASSWORD = 'your_password'
SNOWFLAKE_ACCOUNT = 'your_account'
SNOWFLAKE_WAREHOUSE = 'your_warehouse'
SNOWFLAKE_ROLE = 'your_role'

# Database and schema details for source and target tables
SOURCE_DATABASE = 'db1'
SOURCE_SCHEMA = 'schema1'
SOURCE_TABLE = 'source_table'
TARGET_DATABASE = 'db2'
TARGET_SCHEMA = 'schema2'
TARGET_TABLE = 'target_table'

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    role=SNOWFLAKE_ROLE
)

def get_row_count(database, schema, table):
    query = f"SELECT COUNT(*) FROM {database}.{schema}.{table}"
    cur = conn.cursor()
    cur.execute(query)
    row_count = cur.fetchone()[0]
    cur.close()
    return row_count

def get_table_checksum(database, schema, table):
    query = f"SELECT HASH_AGG(TO_CHAR(*)) AS checksum FROM {database}.{schema}.{table}"
    cur = conn.cursor()
    cur.execute(query)
    checksum = cur.fetchone()[0]
    cur.close()
    return checksum

def validate_row_counts():
    source_row_count = get_row_count(SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE)
    target_row_count = get_row_count(TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE)
    if source_row_count == target_row_count:
        print("Row count check passed.")
        return True
    else:
        print(f"Row count mismatch: Source({source_row_count}) != Target({target_row_count})")
        return False

def validate_table_checksums():
    source_checksum = get_table_checksum(SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE)
    target_checksum = get_table_checksum(TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE)
    if source_checksum == target_checksum:
        print("Table checksum check passed.")
        return True
    else:
        print("Table checksum mismatch detected.")
        return False

def validate_row_checksums(primary_key_column):
    query = f"""
        SELECT src.{primary_key_column}, 
               HASH_AGG(TO_CHAR(*)) AS source_row_checksum,
               tgt.{primary_key_column},
               HASH_AGG(TO_CHAR(*)) AS target_row_checksum
        FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE} AS src
        FULL OUTER JOIN {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE} AS tgt
        ON src.{primary_key_column} = tgt.{primary_key_column}
        WHERE HASH_AGG(TO_CHAR(src.*)) != HASH_AGG(TO_CHAR(tgt.*))
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    if not rows:
        print("Row-level checksum validation passed.")
        return True
    else:
        print(f"Row-level checksum mismatch found in {len(rows)} rows.")
        return False

def validate_random_sample(sample_size=100, primary_key_column="id"):
    query = f"""
        SELECT src.{primary_key_column}, src.*, tgt.*
        FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE} AS src
        JOIN {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE} AS tgt
        ON src.{primary_key_column} = tgt.{primary_key_column}
        WHERE src.{primary_key_column} IN (
            SELECT {primary_key_column} FROM {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE} SAMPLE ({sample_size})
        )
        AND (src.column1 != tgt.column1 OR src.column2 != tgt.column2) -- Add columns to compare here
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    if not rows:
        print("Random sampling check passed.")
        return True
    else:
        print(f"Random sampling mismatch found in {len(rows)} rows.")
        return False

# Main validation function
def main():
    if validate_row_counts():
        if validate_table_checksums():
            print("Data validation passed.")
        else:
            print("Performing row-level validation due to checksum mismatch...")
            primary_key_column = 'your_primary_key_column'  # Update this to your actual primary key
            if validate_row_checksums(primary_key_column):
                print("Data validation passed at row level.")
            else:
                print("Row-level validation detected mismatches.")
    else:
        print("Row count mismatch detected, skipping further checks.")

# Run the validation
main()

# Close connection
conn.close()
