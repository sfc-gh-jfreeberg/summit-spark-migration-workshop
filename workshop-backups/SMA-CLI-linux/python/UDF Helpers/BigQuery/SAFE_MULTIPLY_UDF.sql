-- <copyright file="SAFE_MULTIPLY_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of SAFE_MULTIPLY function from BigQuery
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.SAFE_MULTIPLY_UDF(MULTIPLIER NUMBER, MULTIPLICAND NUMBER)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE 
    WHEN MULTIPLIER IS NULL OR MULTIPLICAND IS NULL THEN NULL
    WHEN MULTIPLIER = 0 OR MULTIPLICAND = 0 THEN 0
    WHEN ABS(MULTIPLIER) > 1E38 OR ABS(MULTIPLICAND) > 1E38 THEN NULL
    WHEN ABS(MULTIPLIER * MULTIPLICAND) > 1E38 THEN NULL
    ELSE MULTIPLIER * MULTIPLICAND
END
$$;
