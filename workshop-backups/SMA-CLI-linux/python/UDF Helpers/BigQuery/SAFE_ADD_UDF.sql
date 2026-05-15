-- <copyright file="SAFE_ADD_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of SAFE_ADD function from BigQuery
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.SAFE_ADD_UDF(ADDEND1 NUMBER, ADDEND2 NUMBER)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE 
    WHEN ADDEND1 IS NULL OR ADDEND2 IS NULL THEN NULL
    WHEN ABS(ADDEND1) > 1E38 OR ABS(ADDEND2) > 1E38 THEN NULL
    WHEN ABS(ADDEND1 + ADDEND2) > 1E38 THEN NULL
    ELSE ADDEND1 + ADDEND2
END
$$;
