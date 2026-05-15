-- <copyright file="FOUND_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The FOUND_UDF() returns TRUE if the last query executed returned any results,
-- returns FALSE if the last query executed returned null.
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.FOUND_UDF() 
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
SELECT (count(*) != 0) FROM TABLE(result_scan(last_query_id()))
$$;