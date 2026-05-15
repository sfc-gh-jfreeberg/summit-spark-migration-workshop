-- <copyright file="JSON_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-----------------------------------------------------------------------------
-- The following UDFs emulate the functionality JSON related functions in BIGQUERY.
-----------------------------------------------------------------------------

-- ====================================================================
-- DESCRIPTION: CONVERT A JSON VALUE TO A SQL INT64 VALUE.
-- EQUIVALENT: LAX_INT64 OPERATOR FROM BIGQUERY.
-- PARAMETERS:
--  VALUE VARIANT - THE JSON VALUE TO BE CONVERTED TO INT64.
-- RETURNS: THE CONVERTED VALUE OR NULL COULD NOT BE CONVERTED.
-- EXAMPLE:
--  INPUT:
--      SELECT PUBLIC.LAX_INT64_UDF(2.3::variant);
--      SELECT PUBLIC.LAX_INT64_UDF(2.8::variant);
--      SELECT PUBLIC.LAX_INT64_UDF('1e2'::variant);
--      SELECT PUBLIC.LAX_INT64_UDF('false::variant);
--      SELECT PUBLIC.LAX_INT64_UDF('test::variant);
--  RESULT:
--      2
--      3
--      100
--      0
--      NULL
-- ====================================================================
CREATE OR REPLACE FUNCTION PUBLIC.LAX_INT64_UDF (VALUE VARIANT)
RETURNS INT
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS $$
  CASE
    WHEN VALUE = true THEN
      1
    WHEN VALUE = false THEN
      0
    ELSE
      TRY_CAST(VALUE::varchar as INT)
END
$$;

-- ====================================================================
-- DESCRIPTION: CONVERT A JSON VALUE TO A SQL BOOLEAN VALUE.
-- EQUIVALENT: LAX_BOOL OPERATOR FROM BIGQUERY.
-- PARAMETERS:
--  VALUE VARIANT - THE JSON VALUE TO BE CONVERTED TO BOOL.
-- RETURNS: THE CONVERTED VALUE OR NULL COULD NOT BE CONVERTED.
-- EXAMPLE:
--  INPUT:
--      SELECT PUBLIC.LAX_BOOL_UDF(1::variant);
--      SELECT PUBLIC.LAX_BOOL_UDF('0'::variant);
--      SELECT PUBLIC.LAX_BOOL_UDF('true'::variant);
--      SELECT PUBLIC.LAX_BOOL_UDF(false::variant);
--  RESULT:
--      TRUE
--      NULL
--      TRUE
--      FALSE
-- ====================================================================
CREATE OR REPLACE FUNCTION PUBLIC.LAX_BOOL_UDF (VALUE VARIANT)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS $$
  CASE
    WHEN TYPEOF(VALUE) = 'VARCHAR' AND (VALUE = '1' OR VALUE = '0') THEN
      NULL
    ELSE
      TRY_TO_BOOLEAN(VALUE::varchar)
END
$$;