-- <copyright file="TD_DAY_OF_CALENDAR_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================
-- RETURNS THE NUMBER OF DAYS THAT HAVE PASSED SINCE 1900-01-01 UP TO THE INPUT DATE, EQUIVALENT TO THE DAY_OF_CALENDAR FUNCTION IN TERADATA
-- PARAMETERS:
--      INPUT: TIMESTAMP_TZ. DATE TO WHICH THE NUMBER OF DAYS SINCE 1900-01-01 WILL BE COUNTED
-- RETURNS:
--      AN INTEGER THAT REPRESENTS THE NUMBER OF DAYS ELAPSED FROM 1900-01-01 TO INPUT
-- ======================================================================
CREATE OR REPLACE FUNCTION PUBLIC.TD_DAY_OF_CALENDAR_UDF(INPUT TIMESTAMP_TZ)
RETURNS INT
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DATEDIFF('DAYS', '1900-01-01', INPUT) + 1
$$;