-- <copyright file="ERROR_STATE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: Returns the error state regardless of how many times it is run, 
-- or where it is run within the scope of the CATCH block.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.ERROR_STATE_UDF() 
RETURNS STRING
LANGUAGE SQL 
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
	SELECT GETVARIABLE('ERROR_STATE')
$$;