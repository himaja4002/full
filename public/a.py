from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, sum as sum_, try_cast, concat, substring, trim, lit, distinct
from snowflake.snowpark.types import StringType

# Snowflake connection setup (if needed externally, though typically for Snowpark in Snowflake, connection is automatic)
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

    # Transformation function for A_PARTY_TKN
    def transform_party_tkn(column):
        return try_cast(
            concat(
                substring(trim(column), 5, 5),
                substring(trim(column), 3, 2),
                substring(trim(column), 1, 2)
            ),
            StringType()
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

        # Apply transformation to A_PARTY_TKN in source and join with target on transformed column
        transformed_source_df = source_df.with_column("transformed_id", transform_party_tkn(source_df["A_PARTY_TKN"]))
        
        # Calculate distinct count of transformed_id in source
        source_distinct_count = transformed_source_df.select("transformed_id").distinct().count()

        # Calculate distinct count of MBR_NR in target
        target_distinct_count = target_df.select("MBR_NR").distinct().count()
        
        if source_distinct_count == target_distinct_count:
            return f"Distinct member count check passed. Count: {source_distinct_count}"
        else:
            return f"Distinct member count mismatch: Source({source_distinct_count}) != Target({target_distinct_count})"

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

    # Perform validation checks
    result = []
    result.append(row_count_check())
    result.append(distinct_member_count_check())
    result.append(table_checksum_check())

    # Return the results as a single string
    return "\n".join(result)
