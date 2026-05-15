-- <copyright file="CONCAT_WS_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: UDF THAT REPRODUCES SPARK-HIVE-DBX CONCAT_WS FUNCTIONALITY
-- PARAMETERS:
--      SEPARATOR: VARCHAR
--      ARGS: ARRAY
-- RETURNS: A VARCHAR JOINED BY THE SEPARATOR OMITTING NULLS IF PRESENT.
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.CONCAT_WS_UDF(SEPARATOR VARCHAR, ARGS ARRAY)
RETURNS VARCHAR
<SnowConvertVersionComment>
AS
$$
  ARRAY_TO_STRING(FILTER(ARGS, ITEM -> ITEM IS NOT NULL), SEPARATOR)
$$;