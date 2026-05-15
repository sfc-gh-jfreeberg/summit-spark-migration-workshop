-- <copyright file="SAFE_POW_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of SAFE_POW function from BigQuery
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.SAFE_POW_UDF(BASE NUMBER, EXPONENT NUMBER)
RETURNS FLOAT
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE 
    WHEN BASE IS NULL OR EXPONENT IS NULL THEN NULL
    WHEN BASE = 0 AND EXPONENT < 0 THEN NULL
    WHEN BASE < 0 AND EXPONENT NOT IN (0, 1, -1) THEN NULL
    WHEN ABS(BASE) > 1E19 OR ABS(EXPONENT) > 1E19 THEN NULL
    WHEN ABS(POWER(BASE, EXPONENT)) > 1E38 THEN NULL
    ELSE POWER(BASE, EXPONENT)
END
$$;