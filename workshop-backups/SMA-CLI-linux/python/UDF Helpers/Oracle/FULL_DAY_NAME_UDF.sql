-- <copyright file="FULL_DAY_NAME_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE FUNCTION PUBLIC.FULL_DAY_NAME_UDF(INPUT DATE, FORMAT_STRING VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DECODE(DAYNAME(INPUT)
        , 'Sun' , 'Sunday'
        , 'Mon' , 'Monday'
        , 'Tue' , 'Tuesday'
        , 'Wed' , 'Wednesday'
        , 'Thu' , 'Thursday'
        , 'Fri' , 'Friday'
        , 'Sat' , 'Saturday'
        ,'None')
$$;