-- <copyright file="GET_TIMEZONE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>
-- ====================================================================

CREATE OR REPLACE FUNCTION PUBLIC.GET_TIMEZONE_UDF()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    return timezone;
$$;