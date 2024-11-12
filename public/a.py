from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, sum as sum_, try_cast, lit, concat, trim, substring

def main(session: Session) -> str:
    # Switch to the secondary role if required
    session.sql("USE ROLE your_secondary_role").collect()  # Replace 'your_secondary_role' with the secondary role name

    # Table details
    SOURCE_DATABASE = 'db1'
    SOURCE_SCHEMA = 'schema1'
    SOURCE_TABLE = 'source_table'
    TARGET_DATABASE = 'db2'
    TARGET_SCHEMA = 'schema2'
    TARGET_TABLE = 'target_table'
    PRIMARY_KEY_COLUMN = 'primary_key_column'  # Replace with actual primary key column

    # Transformation function for A_PARTY_TKN
    def transform_party_tkn(column):
        return try_cast(
            concat(
                substring(trim(column), 5, 5),
                substring(trim(column), 3, 2),
                substring(trim(column), 1, 2)
            ),
            col_type="VARCHAR"
        )

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

    def distinct_member_count_check():
        # Load the tables
        source_df = session.table(f"{SOURCE_DATABASE}.{SOURCE_SCHEMA}.{SOURCE_TABLE}")
        target_df = session.table(f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}")

        # Apply transformation to A_PARTY_TKN in source and join with target on MBR_NR
        joined_df = source_df \
            .join(target_df, 
                  transform_party_tkn(source_df["A_PARTY_TKN"]) == target_df["MBR_NR"]) \
            .filter(source_df["MFP_FILE_ID"] == lit('17272220370-A_PS_CC_Bureau_2024-09-05'))

        # Count distinct MBR_NR after joining
        distinct_count = joined_df.select(target_df["MBR_NR"]).distinct().count()
        
        return f"Distinct MBR_NR count after join: {distinct_count}"

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

    # Perform validation checks
    result = []
    result.append(row_count_check())
    result.append(distinct_member_count_check())
    
    # If row count check passes, continue with further validation
    if "passed" in result[0]:
        result.append(table_checksum_check())
        if "mismatch" in result[2]:
            result.append(row_level_checksum_check())
        else:
            result.append("Table checksum validation passed, no need for row-level check.")
    else:
        result.append("Row count mismatch detected, skipping further checks.")

    # Return the results as a single string
    return "\n".join(result)
