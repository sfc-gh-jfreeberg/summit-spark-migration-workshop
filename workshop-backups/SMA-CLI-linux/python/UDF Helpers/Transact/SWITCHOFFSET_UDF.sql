-- <copyright file="SWITCHOFFSET_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ==========================================================================
-- DESCRIPTION: The OFFSET_FORMATTER(offset_hrs varchar) function formats 
-- offset hours with the correct sign and leading zero.
-- ==========================================================================  
CREATE OR REPLACE FUNCTION PUBLIC.OFFSET_FORMATTER(offset_hrs varchar)
RETURNS varchar
LANGUAGE SQL
<SnowConvertVersionComment>
AS
$$
CASE
   WHEN LEN(offset_hrs) = 0 THEN '+' || '0' || '0'
   WHEN LEN(offset_hrs) = 1 THEN '+' || '0' || offset_hrs
   WHEN LEN(offset_hrs) = 2 THEN
        CASE 
            WHEN LEFT(offset_hrs, 1) = '-' THEN '-' || '0' || RIGHT(offset_hrs, 1)
            ELSE '+' || offset_hrs
END
ELSE offset_hrs
END
$$;

-- =========================================================================================================
-- DESCRIPTION: The SWITCHOFFSET_UDF(source_timestamp TIMESTAMP_TZ, target_tz VARCHAR) function returns a new TIMESTAMP_TZ with
-- the time adjusted according to the specified target_tz offset.

-- Example:
--   Input:
--     SELECT 
--      '2022-10-05 21:59:12.776 +02:00'::timestamp_tz as time_in_paris, 
--       PUBLIC.SWITCHOFFSET_UDF(time_in_paris, '-0600') as time_in_costa_rica;
--
--   Output:
--    ┌────────────────────────────────-┬-───────────────────────────────┐
--    │         time_in_paris           |       time_in_costa_rica       │
--    ├─────────────────────────────────┼────────────────────────────────┤
--    │ 2022-10-05 22:00:24.467 +02:00  | 2022-10-05 14:00:24.467 -06:00 │
--    └─────────────────────────────────┴────────────────────────────────┘
-- =========================================================================================================

CREATE OR REPLACE FUNCTION PUBLIC.SWITCHOFFSET_UDF(source_timestamp TIMESTAMP_TZ, target_tz varchar)
RETURNS TIMESTAMP_TZ
LANGUAGE SQL
<SnowConvertVersionComment>
AS 
$$
WITH tz_values AS (
SELECT 
    RIGHT(source_timestamp::varchar, 5) as source_tz,
    
    REPLACE(source_tz::varchar, ':', '') as source_tz_clean,
    REPLACE(target_tz::varchar, ':', '') as target_tz_clean,
    
    target_tz_clean::integer - source_tz_clean::integer as offset,
    
    RIGHT(offset::varchar, 2) as tz_min,
    PUBLIC.OFFSET_FORMATTER(RTRIM(offset::varchar, tz_min)) as tz_hrs,
    
    
    TIMEADD( hours, tz_hrs::integer, source_timestamp ) as adj_hours,
    TIMEADD( minutes, (LEFT(tz_hrs, 1) || tz_min)::integer, adj_hours::timestamp_tz ) as new_timestamp
    
FROM DUAL)
SELECT 
    (LEFT(new_timestamp, 24) || ' ' || target_tz)::timestamp_tz
FROM tz_values
$$;