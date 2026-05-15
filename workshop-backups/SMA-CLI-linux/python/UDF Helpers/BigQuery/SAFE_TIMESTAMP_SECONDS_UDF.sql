-- <copyright file="SAFE_TIMESTAMP_SECONDS_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================================================================
-- Emulates the BigQuery SAFE.TIMESTAMP_SECONDS function.
-- =============================================================================================
-- DESCRIPTION: Converts a UNIX epoch time in seconds to a TIMESTAMP_NTZ value.
-- EQUIVALENT TO: BigQuery SAFE_TIMESTAMP_SECONDS function.
-- PARAMETERS:
-- seconds NUMBER: The number of seconds since 1970-01-01 00:00:00 UTC.
-- RETURNS: TIMESTAMP_NTZ value, or NULL if the input is out of the supported range.
-- ==============================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.SAFE_TIMESTAMP_SECONDS_UDF(seconds NUMBER)
RETURNS TIMESTAMP_TZ
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE
        WHEN seconds BETWEEN -62135596800 AND 253402300799 THEN
            DATEADD('second', seconds, '1970-01-01' :: TIMESTAMP_TZ)
        ELSE
            NULL
    END
$$;