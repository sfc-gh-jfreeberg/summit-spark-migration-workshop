-- <copyright file="PATINDEX_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The PATINDEX_UDF function returns the starting position of the first occurrence of a pattern 
-- in a specified expression or zeros if the pattern is not found
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.PATINDEX_UDF(PATTERN VARCHAR, EXPRESSION VARCHAR)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
   REGEXP_INSTR(EXPRESSION, REPLACE(REPLACE(PATTERN, '%'), '_', '\.*'))
$$;