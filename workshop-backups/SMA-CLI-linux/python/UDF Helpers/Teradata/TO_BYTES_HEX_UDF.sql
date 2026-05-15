-- <copyright file="TO_BYTES_HEX_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =====================================================================
-- CONVERTS THE INPUT NUMERICAL VALUE BASE 10 TO ITS HEXADECIMAL EQUIVALENT
-- RETURNS AN STRING RESULT
-- PARAMETERS:
--     INPUT: FLOAT. THE NUMERICAL VALUE TO BE CONVERTED TO HEXADECIMAL
-- RETURNS:
--     STRING. THE HEXADECIMAL EQUIVALENT
-- EXAMPLES:
--     SELECT TO_BYTES_HEX_UDF('448');
--     RETURNS 01c0
-- =====================================================================
CREATE OR REPLACE FUNCTION PUBLIC.TO_BYTES_HEX_UDF(INPUT FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
AS
$$
    if (INPUT == null) {
      return null;
    }
    
    var hex = Number(INPUT).toString(16);
    if (hex.length % 2 != 0) {
        hex = "0" + hex;
    }
    return hex;
$$;