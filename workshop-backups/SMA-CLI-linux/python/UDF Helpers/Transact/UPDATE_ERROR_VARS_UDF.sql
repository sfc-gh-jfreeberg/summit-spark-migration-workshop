-- <copyright file="UPDATE_ERROR_VARS_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The UPDATE_ERROR_VARS_UDF() function updates the error variables in environment in order to know when the procedure throws an error. 
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.UPDATE_ERROR_VARS_UDF(LINE STRING,CODE STRING, STATE STRING, MESSAGE STRING, PROC_NAME STRING, SEVERITY STRING) 
RETURNS STRING
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
	select
	SETVARIABLE('ERROR_LINE',LINE) ||
	SETVARIABLE('ERROR_NUMBER',CODE) ||
	SETVARIABLE('ERROR_STATE',STATE) ||
	SETVARIABLE('ERROR_MESSAGE',MESSAGE) ||
	SETVARIABLE('ERROR_PROCEDURE',PROC_NAME) ||
	SETVARIABLE('ERROR_SEVERITY',SEVERITY)
$$;

CREATE OR REPLACE FUNCTION PUBLIC.UPDATE_ERROR_VARS_UDF(MESSAGE STRING, SEVERITY STRING, STATE STRING) 
RETURNS STRING
LANGUAGE SQL
<SnowConvertVersionComment>
AS
$$
	select
	SETVARIABLE('ERROR_STATE',STATE) ||
	SETVARIABLE('ERROR_MESSAGE',MESSAGE) ||
	SETVARIABLE('ERROR_SEVERITY',SEVERITY)
$$;