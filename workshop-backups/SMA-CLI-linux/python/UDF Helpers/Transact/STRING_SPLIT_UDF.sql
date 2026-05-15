-- <copyright file="STRING_SPLIT_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- DESCRIPTION: 
-- The function is designed to replicate the behavior of SQL Server's STRING_SPLIT() function,
-- particularly when the `enable_ordinal` flag is set to true.
-- 
-- In Snowflake, the built-in SPLIT_TO_TABLE function returns a column
-- named 'index', which provides the 1-based ordinal position of each
-- split element. This function renames that column to 'ordinal'
-- and also ignores the 'seq' column, in order to provide consistent results compared to Transact-SQL.
-- 
-- USAGE EXAMPLES:
-- Transact-SQL (SQL Server):
--   SELECT * FROM STRING_SPLIT('Lorem ipsum dolor sit amet.', ' ', 1) WHERE ordinal = 1;
-- Snowflake SQL:
--   SELECT * FROM TABLE(PUBLIC.STRING_SPLIT_UDF('Lorem ipsum dolor sit amet.', ' ')) WHERE ordinal = 1;
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.STRING_SPLIT_UDF(input_string VARCHAR, delimiter VARCHAR)
RETURNS TABLE (
    value VARCHAR,
    ordinal INTEGER
)
LANGUAGE SQL
<SnowConvertVersionComment>
AS
$$
    SELECT
        value,
        index AS ordinal
    FROM
        TABLE(SPLIT_TO_TABLE(input_string, delimiter))
$$;