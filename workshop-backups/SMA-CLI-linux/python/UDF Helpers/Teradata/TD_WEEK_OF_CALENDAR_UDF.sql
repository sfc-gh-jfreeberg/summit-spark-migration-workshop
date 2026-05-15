-- <copyright file="TD_WEEK_OF_CALENDAR_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================
-- RETURNS THE NUMBER OF WEEKS THAT HAVE PASSED SINCE 1900-01-01 UP TO THE INPUT DATE, EQUIVALENT TO THE TD_WEEK_OF_CALENDAR and WEEK_OF_CALENDAR FUNCTIONS IN TERADATA
-- PARAMETERS:
--      INPUT: TIMESTAMP_TZ. DATE TO WHICH THE NUMBER OF WEEKS SINCE 1900-01-01 WILL BE COUNTED
-- RETURNS:
--      AN INTEGER THAT REPRESENTS THE NUMBER OF WEEKS ELAPSED FROM 1900-01-01 TO INPUT
-- ======================================================================
CREATE OR REPLACE FUNCTION PUBLIC.TD_WEEK_OF_CALENDAR_UDF(INPUT TIMESTAMP_TZ)
RETURNS INT
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DATEDIFF('WEEK', '1900-01-01', INPUT)
$$;