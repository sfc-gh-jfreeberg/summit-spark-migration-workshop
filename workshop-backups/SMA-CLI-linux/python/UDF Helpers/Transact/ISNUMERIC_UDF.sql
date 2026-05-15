-- <copyright file="ISNUMERIC_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The ISNUMERIC_UDF() function Determines whether an expression is a valid numeric type. 
-- ISNUMERIC returns 1 when the input expression evaluates to a valid numeric data type; otherwise it returns 0.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.ISNUMERIC_UDF(EXPR VARCHAR)
RETURNS INTEGER
LANGUAGE sql
IMMUTABLE
<SnowConvertVersionComment>
as 
$$
    SELECT CASE WHEN TRY_TO_NUMERIC(EXPR) IS NULL THEN 0 ELSE 1 END
$$;
