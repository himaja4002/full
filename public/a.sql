-- Step 1: Define ignored columns and manual column mapping
WITH source_columns AS (
    SELECT COLUMN_NAME AS source_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TABLE1'  -- Source table
    AND COLUMN_NAME NOT IN ('IGNORE_COLUMN1', 'IGNORE_COLUMN2')  -- Ignored columns
),
target_columns AS (
    SELECT COLUMN_NAME AS target_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TABLE2'  -- Target table
    AND COLUMN_NAME NOT IN ('IGNORE_COLUMN1', 'IGNORE_COLUMN2')  -- Ignored columns
),
-- Manual column mappings for renamed columns
manual_mappings AS (
    SELECT 'PREMIER_COL1' AS source_column, 'PRMR_COL1' AS target_column UNION ALL
    SELECT 'OLD_NAME' AS source_column, 'NEW_NAME' AS target_column
),
-- Map and detect common columns
mapped_columns AS (
    SELECT
        s.source_column,
        CASE
            WHEN s.source_column LIKE 'PREMIER%' THEN REPLACE(s.source_column, 'PREMIER', 'PRMR')  -- Rename logic
            ELSE s.source_column
        END AS mapped_column,
        t.target_column
    FROM source_columns s
    LEFT JOIN target_columns t
        ON CASE
            WHEN s.source_column LIKE 'PREMIER%' THEN REPLACE(s.source_column, 'PREMIER', 'PRMR')
            ELSE s.source_column
          END = t.target_column
    UNION ALL
    SELECT source_column, target_column
    FROM manual_mappings
),
common_columns AS (
    SELECT source_column, target_column
    FROM mapped_columns
    WHERE target_column IS NOT NULL  -- Only validate columns present in both tables
)
-- Step 2: Generate validation query dynamically
SELECT 
    LISTAGG(
        'SUM(CASE WHEN t1.' || source_column || ' <> t2.' || target_column || 
        ' OR (t1.' || source_column || ' IS NULL AND t2.' || target_column || ' IS NOT NULL)' ||
        ' OR (t1.' || source_column || ' IS NOT NULL AND t2.' || target_column || ' IS NULL) THEN 1 ELSE 0 END) AS ' || source_column,
        ', '
    ) AS validation_query
FROM common_columns;
