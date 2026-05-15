-- <copyright file="JSON_EXTRACT_ARRAY_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================================================================
-- The following UDF emulates the functionality of the JSON_QUERY_ARRAY and JSON_EXTRACT_ARRAY built-in function in BigQuery.
-- ======================================================================================================================
-- DESCRIPTION: Extracts an array from a JSON variant using a BigQuery-style JSON path. If the path does not resolve to an array, returns an empty array.
-- EQUIVALENT TO: BigQuery JSON_QUERY_ARRAY and JSON_EXTRACT_ARRAY functions.
-- PARAMETERS:
--   INPUT VARIANT: The JSON variant to extract the array from.
--   PATH VARCHAR (default '$'): The BigQuery-style JSON path specifying the array location.
-- RETURNS: An array extracted from the JSON variant, or an empty array if the path does not resolve to an array.
-- ======================================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.JSON_EXTRACT_ARRAY_UDF(input VARIANT, path VARCHAR DEFAULT NULL)
RETURNS ARRAY
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    with precomputed as
    (
        select
            case when path in ('$.','$') or path is null then input
            else get_path(input, ltrim(path, '$.'))
            end as result
    )
    select
    case typeof(result) when 'ARRAY' then result::array else []  end
    from precomputed
$$;