-- <copyright file="ISDATE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =======================================================================================================
-- Description: The ISDATE function determines whether an expression is a valid date type. 
-- ISDATE returns 1 when the input expression evaluates to a valid date data type; otherwise it returns 0.
-- =======================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.ISDATE_UDF(DATE_VALUE STRING)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
SELECT
    CASE
        WHEN TRY_TO_DATE(DATE_VALUE, 'YYYY/MM/DD') IS NOT NULL OR
             TRY_TO_DATE(DATE_VALUE, 'M/DD/YYYY') IS NOT NULL OR
             TRY_TO_DATE(DATE_VALUE, 'MM/DD/YYYY') IS NOT NULL OR
             TRY_TO_DATE(DATE_VALUE, 'YYYY') IS NOT NULL OR
             TRY_TO_DATE(DATE_VALUE, 'MM.DD.YYYY') IS NOT NULL OR
             TRY_TO_DATE(DATE_VALUE, 'MM/YYYY/DD') IS NOT NULL OR
             TRY_TO_DATE(DATE_VALUE, 'MM-DD-YYYY') IS NOT NULL OR
             TRY_TO_TIMESTAMP_NTZ(DATE_VALUE) IS NOT NULL THEN 1
        ELSE 0
    END
$$;