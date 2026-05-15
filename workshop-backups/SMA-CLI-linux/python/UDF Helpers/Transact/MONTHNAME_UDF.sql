-- <copyright file="MONTHNAME_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: UDF THAT RETURNS THE COMPLETE NAME OF A MONTH BASED ON A TIMESTAMP
-- PARAMETERS:
--      INPUT: TIMESTAMP
-- RETURNS: THE COMPLETE NAME OF A MONTH BASED ON A TIMESTAMP
-- EXAMPLE:
--  SELECT MONTHNAME_UDF('2021-02-14'); -- February
--  SELECT MONTHNAME_UDF('2021-01-14', '2022-02-14'); -- January
-- =============================================

CREATE OR REPLACE FUNCTION PUBLIC.MONTHNAME_UDF(INPUT TIMESTAMP)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DECODE(
       MONTH(INPUT),
        1,'January',
        2,'February',
        3,'March',
        4,'April',
        5,'May',
        6,'June',
        7,'July',
        8,'August',
        9,'September',
        10,'October',
        11,'November',
        12,'December',
       '')
$$;