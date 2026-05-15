-- <copyright file="TIMESTAMP_DIFF_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

------------------------------------------------------------------------------------------
-- The following UDF emulates the functionality of TIMESTAMP_DIFF_UDF function from BigQuery
-- When the difference is calculated in days or hours, BigQuery returns the number of boundaries crossed,
-- so we need to adjust the result by adding or subtracting 1 when the difference is not zero.
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION PUBLIC.TIMESTAMP_DIFF_UDF(PRERESULT NUMBER, UNITY_TIME VARCHAR)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE
        WHEN UPPER(UNITY_TIME) IN ('DAY', 'HOUR') AND PRERESULT = 0 THEN 0
        WHEN UPPER(UNITY_TIME) IN ('DAY', 'HOUR') THEN IFF(PRERESULT > 0, PRERESULT + 1, PRERESULT - 1)
        ELSE PRERESULT
    END
$$;