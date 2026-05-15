-- <copyright file="REGEXP_LIKE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The REGEXP_LIKE_UDF() is created to achieve the functional equivalence in Snowflake 
-- of the REGEXP_LIKE oracle condition.
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.REGEXP_LIKE_UDF(COL STRING, PATTERN STRING, MATCHPARAM STRING) 
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
return COL.match(new RegExp(PATTERN, MATCHPARAM));
$$;

CREATE OR REPLACE FUNCTION PUBLIC.REGEXP_LIKE_UDF(COL STRING, PATTERN STRING) 
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
return COL.match(new RegExp(PATTERN));
$$;
