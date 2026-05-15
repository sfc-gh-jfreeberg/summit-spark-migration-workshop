-- <copyright file="JULIAN_TO_DATE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- DESCRIPTION: UDF THAT TRANSFORMS A JULIAN DATE (YYYYDDD) INTO THE DATE IT REPRESENTS
-- EQUIVALENT: 
-- PARAMETERS:
-- JULIAN DATE: CHAR - THE JULIAN DATE TO TRANSFORM
-- RETURNS: THE DATE REPRESENTATION OF THE JULIAN DATE OR NULL IF CONVERSION IS NOT POSSIBLE
-- EXAMPLE:
-- SELECT JULIAN_TO_DATE_UDF('2022045');
-- RETURNS '2022-02-14'
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.JULIAN_TO_DATE_UDF(JULIAN_DATE CHAR(7))
RETURNS DATE  
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
SELECT
    CASE
        -- Assertion: Must be exactly 7 digits
        WHEN JULIAN_DATE NOT regexp '\\d{7}' THEN NULL
        -- Assertion: Don't accept 0 or negative days in DDD format
        WHEN RIGHT(JULIAN_DATE, 3)::INTEGER < 1 THEN NULL
        -- Assertion: All years have 365 days 
        WHEN RIGHT(JULIAN_DATE, 3)::INTEGER <366 THEN ((LEFT(JULIAN_DATE, 4)||'-01-01')::DATE + RIGHT(JULIAN_DATE, 3)::INTEGER - 1)::DATE
        -- Assertion: If days part is 366, test for leap year (noting that the change of century is not a leap year, but the millenia is)
        WHEN RIGHT(JULIAN_DATE, 3)::INTEGER = 366 THEN
        CASE
        WHEN SUBSTR(JULIAN_DATE, 2,3) = '000' THEN ((LEFT(JULIAN_DATE, 4)||'-01-01')::DATE + RIGHT(JULIAN_DATE, 3)::INTEGER - 1)::DATE -- valid millennia leap year
        WHEN SUBSTR(JULIAN_DATE, 3,2) = '00' THEN NULL -- Century years except millennia are not leap years
        WHEN MOD(LEFT(JULIAN_DATE,4)::INTEGER,4) = 0 THEN ((LEFT(JULIAN_DATE, 4)||'-01-01')::DATE + RIGHT(JULIAN_DATE, 3)::INTEGER - 1)::DATE -- valid leap year
    END
    ELSE 
        -- days part is invalid
        NULL
    END
$$;

-- =========================================================================================================
-- DESCRIPTION: UDF THAT TRANSFORMS A JULIAN TIMESTAMP (YYYYDDDHH24MISS) INTO THE TIMESTAMP IT REPRESENTS
-- EQUIVALENT: 
-- PARAMETERS:
-- JULIAN DATE: CHAR - THE JULIAN TIMESTAMP TO TRANSFORM
-- RETURNS: THE TIMESTAMP REPRESENTATION OF THE JULIAN DATE OR NULL IF CONVERSION IS NOT POSSIBLE
-- EXAMPLE:
-- SELECT JULIAN_TO_TIMESTAMP_UDF('2022045200510');
-- RETURNS '2022-02-14 20:05:10'
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.JULIAN_TO_TIMESTAMP_UDF(JULIAN_DATE CHAR(13))
RETURNS TIMESTAMP  
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
SELECT
    CASE
            -- Assertion: Must be exactly 13 digits
            WHEN JULIAN_DATE NOT regexp '\\d{13}' THEN NULL
            -- Assertion: Don't accept 0 or negative days in DDD format
            WHEN SUBSTRING(JULIAN_DATE, 5, 3)::INTEGER < 1 THEN NULL
            -- Assertion: All years have 365 days 
            WHEN SUBSTRING(JULIAN_DATE, 5, 3)::INTEGER <366 THEN TO_TIMESTAMP(((LEFT(JULIAN_DATE, 4)||'-01-01')::DATE + SUBSTRING(JULIAN_DATE, 5, 3)::INTEGER - 1)::DATE::VARCHAR || RIGHT(JULIAN_DATE,6), 'YYYY-MM-DDHH24MISS')
            -- Assertion: If days part is 366, test for leap year (noting that the change of century is not a leap year, but the millennia is)
            WHEN SUBSTRING(JULIAN_DATE, 5, 3)::INTEGER = 366 THEN
            CASE
            WHEN SUBSTR(JULIAN_DATE, 2,3) = '000' THEN TO_TIMESTAMP(((LEFT(JULIAN_DATE, 4)||'-01-01')::DATE + SUBSTRING(JULIAN_DATE, 5, 3)::INTEGER - 1)::DATE::VARCHAR || RIGHT(JULIAN_DATE,6), 'YYYY-MM-DDHH24MISS')  -- valid millennia leap year
            WHEN SUBSTR(JULIAN_DATE, 3,2) = '00' THEN NULL -- Century years except millennia are not leap years
            WHEN MOD(LEFT(JULIAN_DATE,4)::INTEGER,4) = 0 THEN TO_TIMESTAMP(((LEFT(JULIAN_DATE, 4)||'-01-01')::DATE + SUBSTRING(JULIAN_DATE, 5, 3)::INTEGER - 1)::DATE::VARCHAR || RIGHT(JULIAN_DATE,6), 'YYYY-MM-DDHH24MISS') -- valid leap year
        END
        ELSE 
             -- days part is invalid
             NULL
        END
$$;