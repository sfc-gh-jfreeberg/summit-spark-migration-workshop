-- <copyright file="TIMESTAMP_CONVERTER_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: UDF THAT ALLOWS GETTING THE COMPLETE DAY NAME GIVEN A TIMESTAMP
-- EQUIVALENT:
-- PARAMETERS:
-- INPUT_DATE: DATE
-- RETURNS: THE DAY NAME (IN ENGLISH)
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.DAY_OF_WEEK_LONG_UDF(INPUT_DATE TIMESTAMP)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DECODE(DAYNAME(INPUT_DATE), 'Sun' , 'Sunday'
         , 'Mon' ,'Monday'
         , 'Tue' ,'Tuesday'
         , 'Wed' ,'Wednesday'
         , 'Thu' ,'Thursday'
         , 'Fri','Friday'
         , 'Sat' ,'Saturday'
         ,'None')
$$;

-- =============================================
-- DESCRIPTION: UDF THAT ALLOWS GETTING THE COMPLETE MONTH NAME GIVEN A TIMESTAMP
-- EQUIVALENT:
-- PARAMETERS:
-- INPUT_DATE: DATE
-- RETURNS: THE MONTH NAME (IN ENGLISH)
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.MONTH_NAME_LONG_UDF(INPUT_DATE TIMESTAMP)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DECODE(MONTHNAME(INPUT_DATE)
         , 'Jan' ,'January'
         , 'Feb' ,'February'
         , 'Mar' ,'March'
         , 'Apr' ,'April'
         , 'May' ,'May'
         , 'Jun' ,'June'
         , 'Jul' ,'July'
         , 'Aug' ,'August'
         , 'Sep' ,'September'
         , 'Oct' ,'October'
         , 'Nov' ,'November'
         , 'Dec' ,'December'
         ,'None')
$$;