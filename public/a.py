from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, distinct, try_cast, lit, sum as sum_, when
import json

# Snowflake connection details
connection_parameters = {
    "account": "your_account",
    "user": "your_username",
    "password": "your_password",
    "role": "your_role",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
}

# Initialize a Snowflake Snowpark session
session = Session.builder.configs(connection_parameters).create()

# Table details
SOURCE_DATABASE = 'db1'
SOURCE_SCHEMA = 'schema1'
SOURCE_TABLE = 'source_table'
TARGET_DATABASE = 'db2'
TARGET_SCHEMA = 'schema2'
TARGET_TABLE = 'target_table'
PRIMARY_KEY_COLUMN = 'primary_key_column'  # Replace with actual primary key column

def row_count_check():
    # Row count in source table
    source_df = session.table(f"{SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE}")
    source_count = source_df.count()
    
    # Row count in target table
    target_df = session.table(f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}")
    target_count = target_df.count()
    
    if source_count == target_count:
        print("Row count check passed.")
        return True
    else:
        print(f"Row count mismatch: Source({source_count}) != Target({target_count})")
        return False

def table_checksum_check():
    # Calculating checksum for source table
    source_df = session.table(f"{SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE}")
    source_checksum = source_df.select(sum_(col("column1") + col("column2") + col("column3")).alias("checksum")).collect()[0]["checksum"]
    
    # Calculating checksum for target table
    target_df = session.table(f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}")
    target_checksum = target_df.select(sum_(col("column1") + col("column2") + col("column3")).alias("checksum")).collect()[0]["checksum"]
    
    if source_checksum == target_checksum:
        print("Table checksum check passed.")
        return True
    else:
        print("Table checksum mismatch detected.")
        return False

def row_level_checksum_check():
    source_df = session.table(f"{SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE}")
    target_df = session.table(f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}")

    # Creating row-level checksum for each row and joining on primary key
    source_df = source_df.with_column("row_checksum", sum_(col("column1") + col("column2") + col("column3")))
    target_df = target_df.with_column("row_checksum", sum_(col("column1") + col("column2") + col("column3")))
    
    join_df = source_df.join(target_df, source_df[PRIMARY_KEY_COLUMN] == target_df[PRIMARY_KEY_COLUMN], "full_outer") \
                       .select(source_df[PRIMARY_KEY_COLUMN],
                               source_df["row_checksum"].alias("source_checksum"),
                               target_df["row_checksum"].alias("target_checksum")) \
                       .filter(col("source_checksum") != col("target_checksum"))

    mismatch_count = join_df.count()
    
    if mismatch_count == 0:
        print("Row-level checksum validation passed.")
        return True
    else:
        print(f"Row-level checksum mismatch found in {mismatch_count} rows.")
        return False

def random_sample_check(sample_size=100):
    # Sample random rows from source and target tables
    source_sample = session.table(f"{SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE}").limit(sample_size)
    target_sample = session.table(f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}").limit(sample_size)
    
    # Check if all sampled rows match in source and target
    join_df = source_sample.join(target_sample, source_sample[PRIMARY_KEY_COLUMN] == target_sample[PRIMARY_KEY_COLUMN])
    mismatches = join_df.filter((col("source_column1") != col("target_column1")) |
                                (col("source_column2") != col("target_column2"))).count()
    
    if mismatches == 0:
        print("Random sampling check passed.")
        return True
    else:
        print(f"Random sampling mismatch found in {mismatches} rows.")
        return False

def main():
    # Perform row count check
    if row_count_check():
        # If row count matches, check table-level checksum
        if table_checksum_check():
            print("Data validation passed.")
        else:
            # If table checksum doesn't match, proceed to row-level checksum validation
            print("Performing row-level validation due to checksum mismatch...")
            if row_level_checksum_check():
                print("Data validation passed at row level.")
            else:
                print("Row-level validation detected mismatches.")
    else:
        print("Row count mismatch detected, skipping further checks.")

# Execute the main function
main()

# Close Snowflake session
session.close()
