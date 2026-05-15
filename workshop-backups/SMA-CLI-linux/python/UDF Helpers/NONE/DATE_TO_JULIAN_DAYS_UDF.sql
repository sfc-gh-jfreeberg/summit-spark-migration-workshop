-- <copyright file="DATE_TO_JULIAN_DAYS_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- DESCRIPTION: UDF THAT TRANSFORMS FROM GREGORIAN DATE TO JULIAN DATE (The number of days since January 1, 4712 BC.)
-- EQUIVALENT: 
-- PARAMETERS:
-- input_date: DATE - THE GREGORIAN DATE TO TRANSFORM
-- RETURNS: THE DATE REPRESENTATION OF THE JULIAN DATE
-- EXAMPLE:
--  SELECT DATE_TO_JULIAN_DAYS_UDF(CURRENT_DATE);
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.DATE_TO_JULIAN_DAYS_UDF(input_date DATE)
RETURNS NUMBER  
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DATEDIFF(DAY,TO_DATE('00000101','YYYYMMDD'),TO_DATE('01/01/4712','DD/MM/YYYY')) +
    DATEDIFF(DAY,TO_DATE('00000101','YYYYMMDD'),input_date) + 38 
    // Note: The 38 on the equation marks the differences in days between calendars and must be updated on the year 2099
$$
;