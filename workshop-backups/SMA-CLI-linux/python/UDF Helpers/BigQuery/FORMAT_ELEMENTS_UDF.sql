-- <copyright file="FORMAT_ELEMENTS_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-----------------------------------------------------------------------------
-- The following UDFs emulate the functionality of multiple format elements
-- related to the DATE_FORMAT function from BIGQUERY.
-----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION PUBLIC.CENTURY_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    (TRUNC(YEAR(INPUT) / 100)):: VARCHAR
$$;

CREATE OR REPLACE FUNCTION PUBLIC.DAYNAME_LONG_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR(9)
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    RPAD(
		DECODE(DAYNAME(INPUT)
			, 'Sun' , 'Sunday'
			, 'Mon' , 'Monday'
			, 'Tue' , 'Tuesday'
			, 'Wed' , 'Wednesday'
			, 'Thu' , 'Thursday'
			, 'Fri' , 'Friday'
			, 'Sat' , 'Saturday'
			,'None')
	, 9)
$$;

CREATE OR REPLACE FUNCTION PUBLIC.ISO_YEAR_PART_UDF(INPUT DATE, DIGITS INTEGER)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
	RIGHT(ABS(YEAROFWEEKISO(INPUT)), DIGITS)
$$;

CREATE OR REPLACE FUNCTION PUBLIC.DAY_OF_YEAR_ISO_UDF(INPUT DATE)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
	(DAYOFWEEKISO(INPUT) + (WEEKISO(INPUT) - 1) * 7) :: VARCHAR
$$;