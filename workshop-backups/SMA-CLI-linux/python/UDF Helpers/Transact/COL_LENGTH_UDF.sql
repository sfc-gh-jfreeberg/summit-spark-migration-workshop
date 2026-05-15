-- <copyright file="COL_LENGTH_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ====================================================================== 
-- IT RETURNS THE MAXIMUM DEFINED LENGTH OF A GIVEN COLUMN. 
-- PARAMETERS: 
-- TABLE_NAME_PARAM: Name of the table containing the column; may include the schema.
-- COLUMN_NAME_PARAM: Name of the column whose length is to be retrieved.
-- RETURNS the maximum character length for string columns, or NULL for non-string data types.
-- EQUIVALENT: 
-- TRANSACT EXAMPLE:
--      CREATE TABLE my_table (c1 VARCHAR(50)); 
--      SELECT COL_LENGTH('my_table', 'c1'); -- RETURNS 50 
-- SNOWFLAKE EXAMPLE:
--      CREATE TABLE my_table (c1 VARCHAR(50));
--      SELECT PUBLIC.COL_LENGTH_UDF('my_table', 'c1'); -- RETURNS 50
-- ======================================================================
CREATE OR REPLACE FUNCTION PUBLIC.COL_LENGTH_UDF(table_name_param VARCHAR, column_name_param VARCHAR)
RETURNS NUMBER
LANGUAGE SQL
<SnowConvertVersionComment>
AS
$$
    SELECT CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
        UPPER(COLUMN_NAME) = UPPER(column_name_param)
        AND UPPER(TABLE_NAME) = UPPER(
            CASE
                WHEN POSITION('.' IN table_name_param) > 0 THEN SPLIT_PART(table_name_param, '.', 2)
                ELSE table_name_param
            END
        )
        AND UPPER(TABLE_SCHEMA) = UPPER(
            CASE
                WHEN POSITION('.' IN table_name_param) > 0 THEN SPLIT_PART(table_name_param, '.', 1)
                ELSE CURRENT_SCHEMA()
            END
        )
$$;
