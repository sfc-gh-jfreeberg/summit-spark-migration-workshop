-- <copyright file="SAFE_DIVIDE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of SAFE_DIVIDE function from BigQuery
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.SAFE_DIVIDE_UDF(DIVIDEND NUMBER, DIVISOR NUMBER)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE DIVISOR
    WHEN 0 THEN NULL
    ELSE DIVIDEND / DIVISOR
END
$$;