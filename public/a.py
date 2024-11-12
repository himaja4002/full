from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, sum as sum_, try_cast, lit

def data_validation_with_secondary_role(session: Session) -> str:
    # Switch to the secondary role
    session.sql("USE ROLE your_secondary_role").collect()  # Replace 'your_secondary_role' with the secondary role name

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
            return "Row count check passed."
        else:
            return f"Row count mismatch: Source({source_count}) != Target({target_count})"

    def table_checksum_check():
        # Calculating checksum for source table
        source_df = session.table(f"{SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE}")
        source_checksum = source_df.select(sum_(col("column1") + col("column2") + col("column3")).alias("checksum")).collect()[0]["checksum"]
        
        # Calculating checksum for target table
        target_df = session.table(f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}")
        target_checksum = target_df.select(sum_(col("column1") + col("column2") + col("column3")).alias("checksum")).collect()[0]["checksum"]
        
        if source_checksum == target_checksum:
            return "Table checksum check passed."
        else:
            return "Table checksum mismatch detected."

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
            return "Row-level checksum validation passed."
        else:
            return f"Row-level checksum mismatch found in {mismatch_count} rows."

    def random_sample_check(sample_size=100):
        # Sample random rows from source and target tables
        source_sample = session.table(f"{SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE}").limit(sample_size)
        target_sample = session.table(f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}").limit(sample_size)
        
        # Check if all sampled rows match in source and target
        join_df = source_sample.join(target_sample, source_sample[PRIMARY_KEY_COLUMN] == target_sample[PRIMARY_KEY_COLUMN])
        mismatches = join_df.filter((col("source_column1") != col("target_column1")) |
                                    (col("source_column2") != col("target_column2"))).count()
        
        if mismatches == 0:
            return "Random sampling check passed."
        else:
            return f"Random sampling mismatch found in {mismatches} rows."

    # Perform validation checks
    result = []
    result.append(row_count_check())
    
    # If row count check passes, continue with further validation
    if "passed" in result[0]:
        result.append(table_checksum_check())
        if "mismatch" in result[1]:
            result.append(row_level_checksum_check())
        else:
            result.append("Table checksum validation passed, no need for row-level check.")
    else:
        result.append("Row count mismatch detected, skipping further checks.")

    # Return the results as a single string
    return "\n".join(result)
