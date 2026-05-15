-- <copyright file="TO_CHAR_FORMAT_ELEMENTS_UDF.sql" company="Snowflake Inc">
--        Copyright (C) Mobilize.Net info@mobilize.net - All Rights Reserved
--
--        This file is part of the Mobilize Frameworks, which is
--        proprietary and confidential.
--
--        NOTICE:  All information contained herein is, and remains
--        the property of Mobilize.Net Corporation.
--        The intellectual and technical concepts contained herein are
--        proprietary to Mobilize.Net Corporation and may be covered
--        by U.S. Patents, and are protected by trade secret or copyright law.
--        Dissemination of this information or reproduction of this material
--        is strictly forbidden unless prior written permission is obtained
--        from Mobilize.Net Corporation.
-- </copyright>

-- ===================================================================================
-- The UDFs in this file are used to reproduce the functionality of various format 
-- elements that can be used in the format argument of TO_CHAR(Datetime) and 
-- TO_CHAR(Numeric) in Teradata
-- ===================================================================================

-- ===================================================================================
-- UDFs used for TO_CHAR(Datetime)
-- ===================================================================================

-- ===================================================================================
-- Description: Returns to which century a date belongs to
-- Parameters:
--      1. INPUT: The date to calculate the century from
-- Example:
-- Input:
--      select PUBLIC.CENTURY_UDF(date '1915-02-23');
-- Results:
--      '20'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.CENTURY_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE WHEN MOD(YEAR(INPUT), 100) = 0
    THEN
        (TRUNC(YEAR(INPUT) / 100)):: VARCHAR
    ELSE
        (TRUNC(YEAR(INPUT) / 100) + 1):: VARCHAR
    END
$$;

-- ===================================================================================
-- Description: Returns the complete name of a day either in uppercase, lowercase or with only the first letter in uppercase
-- Parameters:
--      1. INPUT: the date to take the dayname of
--      2. RESULTCASE: The case the result should be represented, accepted values are 'uppercase', 'lowercase' and 'firstOnly'
-- Example:
-- Input:
--      select PUBLIC.DAYNAME_LONG_UDF(date '2021-10-26', 'uppercase');
--      select PUBLIC.DAYNAME_LONG_UDF(date '2021-10-26', 'lowercase');
--      select PUBLIC.DAYNAME_LONG_UDF(date '2021-10-26', 'firstOnly');
-- Results:
--      'TUESDAY'
--      'tuesday'
--      'Tuesday'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.DAYNAME_LONG_UDF(INPUT TIMESTAMP_TZ, RESULTCASE VARCHAR)
RETURNS VARCHAR(10)
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE
    WHEN RESULTCASE = 'uppercase' THEN
        UPPER(DECODE(DAYNAME(INPUT)
         , 'Sun' , 'Sunday'
         , 'Mon' , 'Monday'
         , 'Tue' , 'Tuesday'
         , 'Wed' , 'Wednesday'
         , 'Thu' , 'Thursday'
         , 'Fri' , 'Friday'
         , 'Sat' , 'Saturday'
         ,'None'))
    WHEN RESULTCASE = 'firstOnly' THEN
        DECODE(DAYNAME(INPUT)
         , 'Sun' , 'Sunday'
         , 'Mon' , 'Monday'
         , 'Tue' , 'Tuesday'
         , 'Wed' , 'Wednesday'
         , 'Thu' , 'Thursday'
         , 'Fri' , 'Friday'
         , 'Sat' , 'Saturday'
         ,'None')
    WHEN RESULTCASE = 'lowercase' THEN
        LOWER(DECODE(DAYNAME(INPUT)
         , 'Sun' , 'Sunday'
         , 'Mon' , 'Monday'
         , 'Tue' , 'Tuesday'
         , 'Wed' , 'Wednesday'
         , 'Thu' , 'Thursday'
         , 'Fri' , 'Friday'
         , 'Sat' , 'Saturday'
         ,'None'))
     END
$$;

-- ===================================================================================
-- Description: Overload of DAYNAME_LONG_UDF that applies the default case (only the first letter in uppercase) to the result
-- Parameters:
--      1. INPUT: the date to take the dayname of
-- Example:
-- Input:
--      select PUBLIC.DAYNAME_LONG_UDF(date '2022-06-30');
-- Results:
--      'Thursday'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.DAYNAME_LONG_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR(10)
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    PUBLIC.DAYNAME_LONG_UDF(INPUT, 'firstOnly')
$$;

-- ===================================================================================
-- Description: Emulates the format element DL in Teradata, which represents a date as 'Day, Month DD, YYYY'
-- Parameters:
--      1. INPUT: The date to represent in date long format
-- Example:
-- Input:
--      select PUBLIC.DATE_LONG_UDF(date '2021-10-26');
-- Results:
--      'Tuesday, October 26, 2021'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.DATE_LONG_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    PUBLIC.DAYNAME_LONG_UDF(INPUT, 'firstOnly') || ', ' || TO_CHAR(INPUT, 'MMMM DD, YYYY')
$$;

-- ===================================================================================
-- Description: Returns the complete name of a month either in uppercase, lowercase or with only the first letter in uppercase
-- Parameters:
--      1. INPUT: the date to take the name of the month
--      2. RESULTCASE: The case the result should be represented, accepted values are 'uppercase', 'lowercase' and 'firstOnly'
-- Example:
-- Input:
--      select PUBLIC.FULL_MONTH_NAME_UDF(date '2021-10-26', 'uppercase');
--      select PUBLIC.FULL_MONTH_NAME_UDF(date '2021-10-26', 'lowercase');
--      select PUBLIC.FULL_MONTH_NAME_UDF(date '2021-10-26', 'firstOnly');
-- Results:
--      OCTOBER
--      october
--      October
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.FULL_MONTH_NAME_UDF(INPUT TIMESTAMP_TZ, RESULTCASE VARCHAR)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE
    WHEN RESULTCASE = 'uppercase' THEN
        UPPER(TO_CHAR(INPUT, 'MMMM'))
    WHEN RESULTCASE = 'firstOnly' THEN
        TO_CHAR(INPUT, 'MMMM')
    WHEN RESULTCASE = 'lowercase' THEN
        LOWER(TO_CHAR(INPUT, 'MMMM'))
    END
$$;

-- ===================================================================================
-- Description: Returns the iso year from a given date truncated to the specified number of digits
-- Parameters:
--      1. INPUT: The date to take the iso year from
--      2. DIGITS: The amount of digits wanted in the result
-- Example:
-- Input:
--      select PUBLIC.ISO_YEAR_PART_UDF(date '2021-10-26', 3);
--      select PUBLIC.ISO_YEAR_PART_UDF(date '2021-10-26', 2);
--      select PUBLIC.ISO_YEAR_PART_UDF(date '2021-10-26', 1);
-- Results:
--      '021'
--      '21'
--      '1'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.ISO_YEAR_PART_UDF(INPUT TIMESTAMP_TZ, DIGITS INTEGER)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE WHEN DIGITS > 0  AND DIGITS < 4 THEN
        SUBSTR(YEAROFWEEKISO(INPUT), 5 - DIGITS)
    ELSE
        YEAROFWEEKISO(INPUT)::VARCHAR
    END
$$;

-- ===================================================================================
-- Description: Calculates the julian day of a given date, it is the amount of days that have passed since January 1, 4713 BC
-- Parameters:
--      1. INPUT: The date whose julian day will be calculated
-- Example:
-- Input:
--      select PUBLIC.JULIAN_DAY_UDF(date '2021-10-26');
-- Results:
--      '2459514'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.JULIAN_DAY_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE WHEN MONTH(INPUT) > 2
    THEN
        (TRUNC(DAY(INPUT) + (153 * (MONTH(INPUT) - 3) + 2) / 5 + 365.2425 * (YEAR(INPUT) + 4800) - 32045)):: VARCHAR
    ELSE
        (TRUNC(DAY(INPUT) + (153 * (MONTH(INPUT) + 9) + 2) / 5 + 365.2425 * (YEAR(INPUT) + 4799) - 32045)):: VARCHAR
    END
$$;

-- ===================================================================================
-- Description: Returns the short name of a month either in uppercase, lowercase or with only the first letter in uppercase
-- Parameters:
--      1. INPUT: the date to take the short name of month
--      2. RESULTCASE: The case the result should be represented, accepted values are 'uppercase', 'lowercase' and 'firstOnly'
-- Example:
-- Input:
--      select PUBLIC.MONTH_SHORT_UDF(date '2021-10-26', 'uppercase');
--      select PUBLIC.MONTH_SHORT_UDF(date '2021-10-26', 'lowercase');
--      select PUBLIC.MONTH_SHORT_UDF(date '2021-10-26', 'firstOnly');
-- Results:
--      OCT
--      oct
--      Oct
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.MONTH_SHORT_UDF(INPUT TIMESTAMP_TZ, RESULTCASE VARCHAR)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE
    WHEN RESULTCASE = 'uppercase' THEN
        UPPER(TO_CHAR(INPUT, 'Mon'))
    WHEN RESULTCASE = 'firstOnly' THEN
        TO_CHAR(INPUT, 'Mon')
    WHEN RESULTCASE = 'lowercase' THEN
        LOWER(TO_CHAR(INPUT, 'Mon'))
    END
$$;

-- ===================================================================================
-- Description: Returns the month of a given date represented in roman numerals
-- Parameters:
--      1. INPUT: The date to get the month from
-- Example:
-- Input:
--      select PUBLIC.ROMAN_NUMERALS_MONTH_UDF(date '2021-10-26');
-- Results:
--      'X'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.ROMAN_NUMERALS_MONTH_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DECODE(MONTH(INPUT),
        1, 'I',
        2, 'II',
        3, 'III',
        4, 'IV',
        5, 'V',
        6, 'VI',
        7, 'VII',
        8, 'VIII',
        9, 'IX',
        10, 'X',
        11, 'XI',
        12, 'XII'
    )
$$;

-- ===================================================================================
-- Description: Returns how many seconds have passed since midnight for a given time
-- Parameters:
--      1. INPUT: The time to calculate how many seconds have passed since midnight
-- Example:
-- Input:
--      select PUBLIC.SECONDS_PAST_MIDNIGHT_UDF(time'10:30:45');
-- Results:
--      '37845'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.SECONDS_PAST_MIDNIGHT_UDF(INPUT TIME)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    (HOUR(INPUT) * 3600 + MINUTE(INPUT) * 60 + SECOND(INPUT)):: VARCHAR
$$;

-- ===================================================================================
-- Description: Returns in which week of the month a given date is
-- Parameters:
--      1. INPUT: The date to get which week of the month it belongs to
-- Example:
-- Input:
--      select PUBLIC.WEEK_OF_MONTH_UDF(date '2021-10-26');
-- Results:
--      '4'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.WEEK_OF_MONTH_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    (CEIL(DAY(INPUT) / 7)):: VARCHAR
$$;

-- ===================================================================================
-- Description: Returns the year from a given date truncated to the specified number of digits
-- Parameters:
--      1. INPUT: The date to take the year from
--      2. DIGITS: The amount of digits wanted in the result
-- Example:
-- Input:
--      select PUBLIC.YEAR_PART_UDF(date '2021-10-26', 3);
--      select PUBLIC.YEAR_PART_UDF(date '2021-10-26', 2);
--      select PUBLIC.YEAR_PART_UDF(date '2021-10-26', 1);
-- Results:
--      '021'
--      '21'
--      '1'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.YEAR_PART_UDF(INPUT TIMESTAMP_TZ, DIGITS INTEGER)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE WHEN DIGITS > 0  AND DIGITS < 4 THEN
        SUBSTR(YEAR(INPUT), 5 - DIGITS)
    ELSE
        YEAR(INPUT)::VARCHAR
    END
$$;

-- ===================================================================================
-- Description: Returns the year of a given date with a comma inserted between the first and second digit
-- Parameters:
--      1. INPUT: The date to get the year from
-- Example:
-- Input:
--      select PUBLIC.YEAR_WITH_COMMA_UDF(date '2021-10-26');
-- Results:
--      '2,021'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.YEAR_WITH_COMMA_UDF(INPUT TIMESTAMP_TZ)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    INSERT(YEAR(INPUT), 2, 0, ',')
$$;

-- ===================================================================================
-- UDFs used for TO_CHAR(Numeric)
-- ===================================================================================

-- ===================================================================================
-- Description: Inserts the specified currency text at the given position in the number,
--              or before the first digit to avoid leaving blanks or the number sign between the currency and the number
-- Parameters:
--      1. INPUT: The result of the TO_CHAR(Numeric) that needs the currency inserted
--      2. CURRENCYINDEX: The index to insert the currency at
--      3. CURRENCYVALUE: The text that will be inserted as currency
-- Example:
-- Input:
--      select PUBLIC.INSERT_CURRENCY_UDF(to_char(823, 'S999999'), '1', 'CRC');
-- Results:
--      '+CRC823'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.INSERT_CURRENCY_UDF(INPUT VARCHAR, CURRENCYINDEX INTEGER, CURRENCYVALUE VARCHAR)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE 
    WHEN REGEXP_INSTR(INPUT, '[.0-9]') > CURRENCYINDEX THEN
        INSERT(INPUT, REGEXP_INSTR(INPUT, '[.0-9]'), 0, CURRENCYVALUE)
    ELSE
        INSERT(INPUT, CURRENCYINDEX, 0, CURRENCYVALUE)
    END
$$;

-- ===================================================================================
-- Description: Wraps negative numbers between angle brackets instead of the minus sign
--              It is generated instead of a normal call to TO_CHAR if the PR format element was present in the original Teradata format 
-- Parameters:
--      1. INPUT: The number to convert to varchar
--      2. FORMATARG: The format argument that will be used to convert INPUT to varchar
-- Example:
-- Input:
--      select PUBLIC.WRAP_NEGATIVE_WITH_ANGLE_BRACKETS_UDF(8456, '9999');
--      select PUBLIC.WRAP_NEGATIVE_WITH_ANGLE_BRACKETS_UDF(-8456, '9999');
-- Results:
--      '8456'
--      '<8456>'
-- ===================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.WRAP_NEGATIVE_WITH_ANGLE_BRACKETS_UDF(INPUT NUMBER, FORMATARG VARCHAR)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    CASE WHEN INPUT < 0 THEN
        INSERT(REPLACE(TO_CHAR(INPUT, FORMATARG), '-'), REGEXP_INSTR(REPLACE(TO_CHAR(INPUT, FORMATARG), '-'), '[^ ]'), 0, '<') || '>'
    ELSE
        TO_CHAR(INPUT, FORMATARG)
    END
$$;