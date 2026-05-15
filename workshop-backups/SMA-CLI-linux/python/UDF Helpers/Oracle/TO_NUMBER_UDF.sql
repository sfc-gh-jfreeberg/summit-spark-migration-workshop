-- <copyright file="TO_NUMBER_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- DESCRIPTION: Emulates the Oracle function TO_NUMBER, this overload handles the DEFAULT ON CONVERSION ERROR clause functionality
-- PARAMETERS:
--      INPUT_VALUE: The value to be converted to a number
--      DEFAULT_VALUE: The value to be returned in case of a conversion error
--      FORMAT_STRING: The format to be used for the conversion
--  RETURNS: INPUT_VALUE converted to a number, or DEFAULT_VALUE in case of a conversion error
--  EXAMPLE:
--      Oracle:    SELECT TO_NUMBER('2,00' DEFAULT 10 ON CONVERSION ERROR); -- RETURNS 10
--      Snowflake: SELECT PUBLIC.TO_NUMBER_UDF('2,00', 10); -- RETURNS 10
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.TO_NUMBER_UDF(INPUT_VALUE STRING, DEFAULT_VALUE NUMBER, FORMAT_STRING STRING DEFAULT NULL)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE WHEN FORMAT_STRING IS NULL THEN
    IFF(LENGTH(INPUT_VALUE) = 0, NULL, NVL(TRY_TO_NUMBER(INPUT_VALUE), DEFAULT_VALUE))
ELSE
    IFF(LENGTH(INPUT_VALUE) = 0, NULL, NVL(TRY_TO_NUMBER(INPUT_VALUE, FORMAT_STRING), DEFAULT_VALUE))
END
$$;

-- =========================================================================================================
-- DESCRIPTION: Emulates the Oracle function TO_NUMBER
-- PARAMETERS:
--      INPUT_VALUE: The value to be converted to a number
--      FORMAT_STRING: The format to be used for the conversion
--  RETURNS: INPUT_VALUE converted to a number or an exception in case of a conversion error
--  EXAMPLE:
--      Oracle:    SELECT TO_NUMBER('2,00', '9G99'); -- RETURNS 200
--      Snowflake: SELECT PUBLIC.TO_NUMBER_UDF('2,00', '9G99'); -- RETURNS 200
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.TO_NUMBER_UDF(INPUT_VALUE STRING, FORMAT_STRING STRING DEFAULT NULL)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE WHEN FORMAT_STRING IS NULL THEN
    IFF(LENGTH(INPUT_VALUE) = 0, NULL, TO_NUMBER(INPUT_VALUE))
ELSE
    IFF(LENGTH(INPUT_VALUE) = 0, NULL, TO_NUMBER(INPUT_VALUE, FORMAT_STRING))
END
$$;