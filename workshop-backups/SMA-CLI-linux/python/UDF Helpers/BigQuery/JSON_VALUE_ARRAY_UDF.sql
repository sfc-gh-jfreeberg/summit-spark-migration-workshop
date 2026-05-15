-- <copyright file="JSON_VALUE_ARRAY_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================================================================
-- The following UDFs emulate the functionality JSON_VALUE_ARRAY built-in function in Bigquery.
-- ======================================================================================================================
-- DESCRIPTION: Extracts an array of scalar values from a JSON string using a BigQuery-style JSON path.
-- EQUIVALENT TO: BigQuery JSON_VALUE_ARRAY function.
-- PARAMETERS:
--   JSON_STRING VARCHAR: The JSON string to extract values from.
--   JSON_PATH VARCHAR (default '$'): The BigQuery-style JSON path specifying the array location.
-- RETURNS: An array of scalar values, or NULL if extraction fails or the path does not resolve to a scalar array.
-- ======================================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.JSON_VALUE_ARRAY_UDF(
    json_string VARCHAR, 
    json_path VARCHAR DEFAULT '$'
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    if (JSON_STRING === null) return null;
    let jsonObj;
    try {
        jsonObj = JSON.parse(JSON_STRING);
    } catch (e) {
        return null;
    }

    let currentVal = jsonObj;
    if (JSON_PATH !== '$') {
        const pathWithoutRoot = JSON_PATH.substring(2);
        const keys = pathWithoutRoot.match(/"[^"]+"|[^\.]+/g) || [];
        
        for (const key of keys) {
            const cleanKey = key.replace(/"/g, '');
            if (currentVal === null || typeof currentVal !== 'object' || !currentVal.hasOwnProperty(cleanKey)) {
                return null;
            }
            currentVal = currentVal[cleanKey];
        }
    }
    
    if (Array.isArray(currentVal) && currentVal.every(item => item !== null && typeof item !== 'object')) {
        return currentVal;
    } 
    
    return null;
$$;