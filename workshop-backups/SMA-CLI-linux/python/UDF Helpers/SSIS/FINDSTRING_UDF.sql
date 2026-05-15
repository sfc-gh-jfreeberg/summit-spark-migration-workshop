-- <copyright file="FINDSTRING_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: Emulates SSIS FINDSTRING function for dynamic search strings
-- EQUIVALENT: SSIS FINDSTRING function with runtime pattern escaping
-- PARAMETERS:
--   CHARACTER_EXPRESSION: The string to search within
--   SEARCH_STRING: The substring to find (will be escaped for regex)
--   OCCURRENCE: Which occurrence to find (1-based)
-- RETURNS: 1-based position of the occurrence, or 0 if not found
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.FINDSTRING_UDF(
    CHARACTER_EXPRESSION VARCHAR,
    SEARCH_STRING VARCHAR,
    OCCURRENCE INT
)
RETURNS INT
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
  CASE
    WHEN OCCURRENCE IS NULL OR OCCURRENCE <= 0 THEN 0
    WHEN CHARACTER_EXPRESSION IS NULL OR SEARCH_STRING IS NULL THEN NULL
    ELSE
      REGEXP_INSTR(
        CHARACTER_EXPRESSION,
        REGEXP_REPLACE(
          REPLACE(SEARCH_STRING, '\\', '\\\\'),
          '([|.*+?()\\[\\]{}^$])',
          '\\\\\\1'
        ),
        1,
        OCCURRENCE
      )
  END
$$;

