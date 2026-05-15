-- <copyright file="DAYNAME_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: UDF THAT RETURNS THE COMPLETE NAME OF A DAY BASED ON A TIMESTAMP
-- PARAMETERS:
--      INPUT: TIMESTAMP
-- RETURNS: THE COMPLETE NAME OF A MONTH BASED ON A TIMESTAMP
-- EXAMPLE:
--  SELECT DAYNAME_UDF('2021-02-14'); -- February
--  SELECT DAYNAME_UDF('2021-01-14', '2022-02-14'); -- January
-- =============================================

CREATE OR REPLACE FUNCTION PUBLIC.DAYNAME_UDF(INPUT TIMESTAMP)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DECODE(
       DAYNAME(INPUT),
        'Mon', 'Monday',
        'Tue', 'Tuesday',
        'Wed', 'Wednesday',
        'Thu', 'Thursday',
        'Fri', 'Friday',
        'Sat', 'Saturday',
        'Sun', 'Sunday',
       '')
$$;