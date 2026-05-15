-- <copyright file="ARRAY_TO_STRING_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================================================================
-- The following UDF emulates the functionality of the ARRAY_TO_STRING built-in function in BigQuery.
-- ======================================================================================================================
-- DESCRIPTION: Concatenates array elements into a single string, separated by a specified delimiter.
-- EQUIVALENT TO: BigQuery ARRAY_TO_STRING function.
-- PARAMETERS:
--   INPUT_ARRAY ARRAY: The array of elements to concatenate.
--   DELIMITER STRING: The string to insert between each element.
--   NULL_REPLACEMENT STRING: The string to use for null elements in the array.
-- RETURNS: A string with array elements joined by the delimiter, using the null replacement for nulls.
-- ======================================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.ARRAY_TO_STRING_UDF(
    input_array ARRAY,
    delimiter STRING,
    null_replacement STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    if (!INPUT_ARRAY || INPUT_ARRAY.length === 0) {
        return null;
    }
    
    var result = [];
    for (var i = 0; i < INPUT_ARRAY.length; i++) {
        var element = INPUT_ARRAY[i];
        result.push(element === null || element === undefined ? NULL_REPLACEMENT : String(element));
    }
    
    return result.join(DELIMITER);
$$;