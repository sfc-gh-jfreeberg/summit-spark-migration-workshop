-- <copyright file="NVP_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: EQUALS NVP TERADATA'S FUNCTION
-- PARAMETER: STRING INSTRING
-- PARAMETER: STRING NAME_TO_SEARCH
-- PARAMETER: STRING NAME_DELIMITERS
-- PARAMETER: STRING VALUE_DELIMITERS
-- PARAMETER: FLOAT OCCURRENCE
-- EXAMPLE:
--    SELECT PUBLIC.NVP_UDF('entree=-orange chicken&entree+.honey salmon', 'entree', '&', '=- +.', 1);
--    RETURNS orange chicken
-- TERADATA EQUIVALENT: 
-- EXAMPLE:
--    SELECT NVP('entree=-orange chicken&entree+.honey salmon', 'entree', '&', '=- +.', 1);
--    RETURNS orange chicken
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.NVP_UDF(INSTRING VARCHAR, NAME_TO_SEARCH VARCHAR, NAME_DELIMITERS VARCHAR, VALUE_DELIMITERS VARCHAR, OCCURRENCE FLOAT)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    NAME_DELIMITERS = NAME_DELIMITERS.replaceAll("\\ ","DELIMITER_SPACE");
    VALUE_DELIMITERS = VALUE_DELIMITERS.replaceAll("\\ ","DELIMITER_SPACE");
    NAME_DELIMITERS = NAME_DELIMITERS.match(/[^ ]+/g);
    VALUE_DELIMITERS = VALUE_DELIMITERS.match(/[^ ]+/g);
    for(var NAME_DELIM of NAME_DELIMITERS)
    {
        NAME_DELIM = NAME_DELIM.replaceAll("DELIMITER_SPACE", " ");
        INSTRING = INSTRING.replaceAll(NAME_DELIM, "NAME_DELIMITER");
    }
    for(var VALUE_DELIM of VALUE_DELIMITERS)
    {
        VALUE_DELIM = VALUE_DELIM.replaceAll("DELIMITER_SPACE", " ");
        INSTRING = INSTRING.replaceAll(VALUE_DELIM, "VALUE_DELIMITER");
    }
    KEY_VALUE_PAIRS = INSTRING.split("NAME_DELIMITER");
    FILTERED_PAIRS = KEY_VALUE_PAIRS.filter(pair => pair.startsWith(NAME_TO_SEARCH));
    if (FILTERED_PAIRS.length >= OCCURRENCE)
    {
        RESULT_PAIR = FILTERED_PAIRS[OCCURRENCE - 1];
        return RESULT_PAIR.split("VALUE_DELIMITER")[1];
    }

    return null;
$$;