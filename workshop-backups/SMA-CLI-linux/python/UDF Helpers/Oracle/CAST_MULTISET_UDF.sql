-- <copyright file="CAST_MULTISET_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The CAST_MULTISET_UDF(array) function takes an array and converts it to text with braces ({}).
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.CAST_MULTISET_UDF (array array)
RETURNS TEXT
LANGUAGE JAVASCRIPT
IMMUTABLE 
<SnowConvertVersionComment>
AS
$$
    return '{' + ARRAY.join(',') + '}';
$$;