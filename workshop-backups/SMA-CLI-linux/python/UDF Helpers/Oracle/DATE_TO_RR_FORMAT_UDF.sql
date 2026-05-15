-- <copyright file="DATE_TO_RR_FORMAT_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- DESCRIPTION: UDF THAT TRANSFORMS FROM DATE TO ORACLE RR DATETIME FORMAT ELEMENT
-- EQUIVALENT: to_date('17-NOV-99','DD-MON-RR')
-- PARAMETERS:
-- input_date: DATE
-- RETURNS: THE INPUT DATE WITH ADJUSTED YEARS TO RR FORMAT
-- EXAMPLE:
--  SELECT CONVERT_DATE_WITH_RR_FORMAT_UDF(to_date('17-NOV-99','DD-MON-YY'));
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.DATE_TO_RR_FORMAT_UDF(INPUT_DATE DATE)
RETURNS DATE
LANGUAGE SQL
<SnowConvertVersionComment>
as
$$
    IFF( MOD(YEAR(INPUT_DATE), 100) <= 49,
        IFF(MOD(YEAR(CURRENT_DATE()), 100) <= 49, INPUT_DATE, DATE_FROM_PARTS( (TRUNC(YEAR(CURRENT_DATE())/100)+1)*100 + MOD(YEAR(INPUT_DATE), 100), MONTH(INPUT_DATE), DAY(INPUT_DATE))),
        IFF(MOD(YEAR(CURRENT_DATE()), 100) <= 49, DATE_FROM_PARTS( (TRUNC(YEAR(CURRENT_DATE())/100)-1)*100 + MOD(YEAR(INPUT_DATE), 100), MONTH(INPUT_DATE), DAY(INPUT_DATE)), INPUT_DATE))
$$;
