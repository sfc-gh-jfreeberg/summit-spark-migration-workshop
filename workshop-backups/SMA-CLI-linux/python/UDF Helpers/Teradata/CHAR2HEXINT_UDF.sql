-- <copyright file="CHAR2HEXINT_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: RETURNS THE HEXADECIMAL REPRESENTATION FOR A CHARACTER STRING.
-- EQUIVALENT: TERADATA'S CHAR2HEXINT FUNCTIONALITY
-- PARAMETERS:
-- INPUT_STRING: VARCHAR - STRING TO CONVERT
-- RETURNS: HEXADECIMAL REPRESENTATION OF THE STRING
-- EXAMPLE:
--  SELECT CHAR2HEXINT_UDF('123');
--  RETURNS 003100320033
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.CHAR2HEXINT_UDF (INPUT_STRING VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  return INPUT_STRING.split('').map( e => "00" + e.charCodeAt(0).toString(16) ).join('');
$$
;