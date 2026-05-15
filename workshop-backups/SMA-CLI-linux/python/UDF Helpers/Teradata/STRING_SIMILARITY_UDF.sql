-- <copyright file="STRING_SIMILARITY_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: RETURNS THE JARO-WINKER SIMILARITY BETWEEN TWO STRINGS.
-- EQUIVALENT: TERADATA'S JARO_WINKER FUNCTIONALITY
-- PARAMETERS:
-- STRING1: VARCHAR - STRING TO COMPARED
-- STRING2: VARCHAR - STRING TO COMPARED
-- RETURNS: THE JARO-WINKER SIMILARITY
-- EXAMPLE:
--  SELECT JAROWINKLER_UDF('Ich weiß nicht', 'Ich weiss nicht');
--  RETURNS 0.950000
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.JAROWINKLER_UDF (string1 VARCHAR, string2 VARCHAR)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    IFNULL(JAROWINKLER_SIMILARITY(string1, string2), 0) / 100
$$
;