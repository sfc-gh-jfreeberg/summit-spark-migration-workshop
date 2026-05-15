// <copyright file="STR_UDF.sql" company="Snowflake Inc">
//        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
// </copyright>

-- =========================================================================================================
-- DESCRIPTION: The STR_UDF() is a template for translating the functionality of SQL Server STR() to
--   Snowflake when it's used with one or two optional parameters
-- EQUIVALENT: 
--   TRANSACT STR(numeric_value, optional_param1, optional_param2)
-- EXAMPLES: 
--   SELECT STR_UDF(numeric_value, number_format) 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.STR_UDF(FLOAT_EXPR FLOAT, FORMAT VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    TRIM(TRIM(SELECT TO_CHAR(FLOAT_EXPR, FORMAT)), '.')
$$;

CREATE OR REPLACE FUNCTION PUBLIC.STR_UDF(FLOAT_EXPR FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    STR_UDF(FLOAT_EXPR, '999999999999999999')
$$;