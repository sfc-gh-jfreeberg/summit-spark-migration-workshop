-- <copyright file="SAFE_SUBTRACT_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of SAFE_SUBTRACT function from BigQuery
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.SAFE_SUBTRACT_UDF(MINUEND NUMBER, SUBTRAHEND NUMBER)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE 
    WHEN MINUEND IS NULL OR SUBTRAHEND IS NULL THEN NULL
    WHEN ABS(MINUEND) > 1E38 OR ABS(SUBTRAHEND) > 1E38 THEN NULL
    WHEN ABS(MINUEND - SUBTRAHEND) > 1E38 THEN NULL
    ELSE MINUEND - SUBTRAHEND
END
$$;
