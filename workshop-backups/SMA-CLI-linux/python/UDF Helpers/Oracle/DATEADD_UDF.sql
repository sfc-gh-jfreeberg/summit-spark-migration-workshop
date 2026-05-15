-- <copyright file="DATEADD_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- DESCRIPTION: The DATEADD_UDF() is used as a template for all the cases when there is an addition
-- between a DATE and an UNKNOWN type like a float.
-- EQUIVALENT: ORACLE DATE + FLOAT , TIMESTAMP + FLOAT, FLOAT + DATE , FLOAT +TIMESTAMP
-- PARAMETERS:
-- SCENARIO 1:
--      FIRST_PARAM: DATE OR TIMESTAMP
--      SECOND_PARAM: FLOAT to be added to FIRST_PARAM, the units represent days
--  RETURNS: The DATE result of the sum of the inputs
--  EXAMPLE:
--      SELECT DATEADD_UDF('2022-02-14',6); -- SELECT TO_DATE('2022-02-14','YYYY-MM-DD') + 6 FROM DUAL; -- RETURNS '2022-02-20'
-- SCENARIO 2:
--      FIRST_PARAM: FLOAT to be added to SECOND_PARAM, the units represent days
--      SECOND_PARAM: DATE OR TIMESTAMP
--  RETURNS: The DATE result of the sum of the inputs
--  EXAMPLE:
--      SELECT DATEADD_UDF(6,'2022-02-14 15:31:00'); -- SELECT 6 + TO_TIMESTAMP('2022-02-14 15:31:00','YYYY-MM-DD HH24:MI:SS') FROM DUAL; -- RETURNS '2022-02-20'

-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.DATEADD_UDF(FIRST_PARAM DATE, SECOND_PARAM FLOAT)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
SELECT FIRST_PARAM + SECOND_PARAM::NUMBER
$$;

CREATE OR REPLACE FUNCTION PUBLIC.DATEADD_UDF(FIRST_PARAM FLOAT, SECOND_PARAM DATE)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
SELECT FIRST_PARAM::NUMBER + SECOND_PARAM
$$;

CREATE OR REPLACE FUNCTION PUBLIC.DATEADD_UDF(FIRST_PARAM TIMESTAMP, SECOND_PARAM NUMBER)
RETURNS TIMESTAMP
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
SELECT DATEADD(ms, SECOND_PARAM*3600000*24, FIRST_PARAM)
$$;

CREATE OR REPLACE FUNCTION PUBLIC.DATEADD_UDF(FIRST_PARAM FLOAT, SECOND_PARAM TIMESTAMP)
RETURNS TIMESTAMP
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
SELECT DATEADD(day, FIRST_PARAM,SECOND_PARAM)
$$;

-- =========================================================================================================
-- HELPER FUNCTION: Shared logic for TIMESTAMP arithmetic with VARCHAR input
-- This helper function contains the shared logic for parsing VARCHAR timestamp differences
-- and applying them to timestamps with a configurable sign (1 for addition, -1 for subtraction)
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.VARCHAR_TO_TIMESTAMP_UDF(TS TIMESTAMP, VARCHAR_DIFF VARCHAR, SIGN_MULTIPLIER NUMBER)
RETURNS TIMESTAMP
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    WITH
    clean_input (sign, cleaned_string) AS (
      -- Remove all spaces from input string while preserving the sign
      SELECT 
        SUBSTRING(VARCHAR_DIFF, 1, 1) AS sign,
        SUBSTRING(VARCHAR_DIFF, 1, 1) || REPLACE(SUBSTRING(VARCHAR_DIFF, 2), ' ', '') AS cleaned_string
    ),
    convert_string_float (difference) AS (
      SELECT 
        CASE WHEN sign = '-' THEN -1 ELSE 1 END *
        (
          -- Extract days (positions 2-10 after cleaning)
          TRY_CAST(TRIM(SUBSTRING(cleaned_string, 2, 9)) AS FLOAT) +
          -- Extract hours and convert to days (positions 11-12 after cleaning)
          (TRY_CAST(SUBSTRING(cleaned_string, 11, 2) AS FLOAT) / 24.0) +
          -- Extract minutes and convert to days (positions 14-15 after cleaning, skip colon at 13)
          (TRY_CAST(SUBSTRING(cleaned_string, 14, 2) AS FLOAT) / 1440.0) +
          -- Extract seconds and convert to days (positions 17-18 after cleaning, skip colon at 16)
          (TRY_CAST(SUBSTRING(cleaned_string, 17, 2) AS FLOAT) / 86400.0) +
          -- Extract fractional seconds and convert to days (positions 20-27 after cleaning, skip dot at 19)
          (TRY_CAST(SUBSTRING(cleaned_string, 20, 8) AS FLOAT) / 86400000.0)
        )
      FROM clean_input
    )
SELECT DATEADD(ms, difference * 3600000 * 24 * SIGN_MULTIPLIER, TS) FROM convert_string_float
$$;

CREATE OR REPLACE FUNCTION PUBLIC.DATEADD_UDF(FIRST_PARAM TIMESTAMP, SECOND_PARAM VARCHAR)
RETURNS TIMESTAMP
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  SELECT PUBLIC.VARCHAR_TO_TIMESTAMP_UDF(FIRST_PARAM, SECOND_PARAM, 1)
$$;