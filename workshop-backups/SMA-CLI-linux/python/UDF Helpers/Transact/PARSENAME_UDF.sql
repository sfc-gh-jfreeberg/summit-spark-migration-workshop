-- <copyright file="PARSENAME_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The PARSENAME() function gets the PART_Number index of a String
-- separated by '.'.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.PARSENAME_UDF(STR VARCHAR, PART_NUMBER INT)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  SELECT SPLIT_PART(STR,'.', -1 * PART_NUMBER)
$$;