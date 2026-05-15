-- <copyright file="ERROR_SEVERITY_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The ERROR_SEVERITY() function gets a value indicating the severity of an error. Default value will
-- always be 16.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.ERROR_SEVERITY_UDF() 
RETURNS STRING
LANGUAGE SQL 
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
	SELECT GETVARIABLE('ERROR_SEVERITY')
$$;
