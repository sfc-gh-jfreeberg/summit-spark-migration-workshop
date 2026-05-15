-- <copyright file="CAST_DATE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The CAST_DATE_UDF() function takes a DATE in string format and returns the string as a date
-- with the specified format.  Is used to avoid a format error when the NLS_DATE_FORMAT has been changed
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.CAST_DATE_UDF(DATESTR STRING) 
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
	SELECT TO_DATE(DATESTR,'YYYY-MM-DD"T"HH24:MI:SS.FF')
$$;