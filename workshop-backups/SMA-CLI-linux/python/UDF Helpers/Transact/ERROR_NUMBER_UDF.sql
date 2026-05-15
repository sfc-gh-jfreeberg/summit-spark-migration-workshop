-- <copyright file="ERROR_NUMBER_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The ISNUMERIC() function Determines whether an expression is a valid numeric type. 
-- ISNUMERIC returns 1 when the input expression evaluates to a valid numeric data type; otherwise it returns 0.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.ERROR_NUMBER_UDF() 
RETURNS STRING
LANGUAGE SQL 
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
	SELECT GETVARIABLE('ERROR_NUMBER')
$$;