-- <copyright file="INIT_CURSOR_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE FUNCTION PUBLIC.INIT_CURSOR_UDF(NAME VARCHAR, QUERY VARCHAR)
RETURNS OBJECT
<SnowConvertVersionComment>
AS
$$
  SELECT OBJECT_CONSTRUCT('NAME', NAME, 'ROWCOUNT', -1, 'QUERY', QUERY, 'ISOPEN', FALSE, 'FOUND', NULL, 'NOTFOUND', NULL)
$$;