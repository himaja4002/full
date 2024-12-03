import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session):
    # Step 1: Use secondary roles
    session.sql("USE SECONDARY ROLES ALL").collect()
    
    # Step 2: Source and target table names
    source_table = "Table1"
    target_table = "Table2"

    # Step 3: Fetch column names dynamically
    source_columns = session.table(source_table).schema.names
    target_columns = session.table(target_table).schema.names

    # Step 4: Generate column mappings dynamically
    column_mapping = []
    for col_name in source_columns:
        # Handle "PREMIER" to "PRMR" pattern
        if col_name.startswith("PREMIER"):
            mapped_col = col_name.replace("PREMIER", "PRMR")
            if mapped_col in target_columns:
                column_mapping.append((col_name, mapped_col))
        # Check if the column exists with the same name in both tables
        elif col_name in target_columns:
            column_mapping.append((col_name, col_name))
        # Additional custom logic for columns not matching directly
        else:
            # Example: If other columns have a prefix or suffix
            possible_match = col_name.replace("_SRC", "_TGT")  # Replace with your convention
            if possible_match in target_columns:
                column_mapping.append((col_name, possible_match))

    # Step 5: Validate columns
    validation_results = []
    for source_col, target_col in column_mapping:
        # Build a query to compare columns
        mismatch_query = session.sql(f"""
            SELECT
                '{source_col}' AS column_name,
                COUNT(*) AS mismatched_count
            FROM {source_table} t1
            FULL OUTER JOIN {target_table} t2
                ON t1.AS_OF_DT = t2.AS_OF_DT  -- Replace with your join condition
            WHERE t1.{source_col} <> t2.{target_col}
                OR (t1.{source_col} IS NULL AND t2.{target_col} IS NOT NULL)
                OR (t1.{source_col} IS NOT NULL AND t2.{target_col} IS NULL)
        """)
        
        # Execute and fetch results
        result = mismatch_query.collect()[0]
        validation_results.append(result)
    
    # Step 6: Convert results to a Snowpark DataFrame and display
    result_df = session.create_dataframe(validation_results, schema=["Column", "Mismatched_Count"])
    result_df.show()

    # Return results
    return result_df

import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    # Use secondary roles and set warehouse
    session.sql("USE SECONDARY ROLES ALL").collect()
    session.sql("USE WAREHOUSE my_large_warehouse").collect()  # Use a larger warehouse
    
    # Set a higher timeout limit
    session.sql("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 7200").collect()
    
    # Source and target table names
    source_table = "Table1"
    target_table = "Table2"

    # Fetch column names
    source_columns = session.table(source_table).schema.names
    target_columns = session.table(target_table).schema.names

    # Identify common columns dynamically
    column_mapping = [(col, col) for col in source_columns if col in target_columns]

    # Validate columns in chunks
    chunk_size = 10
    validation_results = []
    for i in range(0, len(column_mapping), chunk_size):
        chunk = column_mapping[i:i + chunk_size]
        for source_col, target_col in chunk:
            mismatch_query = session.sql(f"""
                SELECT
                    '{source_col}' AS column_name,
                    COUNT(*) AS mismatched_count
                FROM {source_table} t1
                FULL OUTER JOIN {target_table} t2
                    ON t1.AS_OF_DT = t2.AS_OF_DT
                WHERE t1.{source_col} <> t2.{target_col}
                    OR (t1.{source_col} IS NULL AND t2.{target_col} IS NOT NULL)
                    OR (t1.{source_col} IS NOT NULL AND t2.{target_col} IS NULL)
            """)
            result = mismatch_query.collect()[0]
            validation_results.append(result)

    # Return validation results as a DataFrame
    return session.create_dataframe(validation_results, schema=["Column", "Mismatched_Count"])

