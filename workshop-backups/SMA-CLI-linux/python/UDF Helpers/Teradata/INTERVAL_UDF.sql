-- <copyright file="INTERVAL_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- Description: UDF for converting an Interval to Months
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.INTERVAL_TO_MONTHS_UDF
(INPUT_VALUE VARCHAR())
RETURNS INTEGER
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE WHEN SUBSTR(INPUT_VALUE,1,1) = '-' THEN
   12 * CAST(SUBSTR(INPUT_VALUE,1 , POSITION('-', INPUT_VALUE,2)-1) AS INTEGER)
   - CAST(SUBSTR(INPUT_VALUE,POSITION('-', INPUT_VALUE)+1) AS INTEGER)
ELSE
   12 * CAST(SUBSTR(INPUT_VALUE,1 , POSITION('-', INPUT_VALUE,2)-1) AS INTEGER)
   + CAST(SUBSTR(INPUT_VALUE,POSITION('-', INPUT_VALUE)+1) AS INTEGER)
END
$$;

-- =============================================
-- Description: UDF for converting Months to an Interval
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.MONTHS_TO_INTERVAL_UDF
(INPUT_PART VARCHAR(30), INPUT_VALUE NUMBER)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
DECODE(INPUT_PART,
                'YEAR',                (INPUT_VALUE/(12))::varchar,
                'YEAR TO MONTH',       TRUNC(INPUT_VALUE / 12) ||'-'|| MOD(INPUT_VALUE, 12)::varchar,     
                'MONTH',               INPUT_VALUE::varchar
)
$$;

-- =============================================
-- Description: UDF for converting an Interval to Seconds
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.INTERVAL_TO_SECONDS_UDF
(INPUT_PART VARCHAR(30), INPUT_VALUE VARCHAR())
RETURNS DECIMAL(20,6)
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE WHEN SUBSTR(INPUT_VALUE,1,1) = '-' THEN
   DECODE(INPUT_PART,
           'DAY',              86400 * INPUT_VALUE, 
           'DAY TO HOUR',      86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(' ', INPUT_VALUE)-1) AS DECIMAL(10,0)) 
                               - 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION(' ', INPUT_VALUE)+1) AS DECIMAL(10,0)),
           'DAY TO MINUTE',    86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(' ', INPUT_VALUE)-1) AS INTEGER) 
                               - 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION(' ', INPUT_VALUE)+1, POSITION(':', INPUT_VALUE)-POSITION(' ', INPUT_VALUE)-1) AS INTEGER) 
                               - 60 * CAST(SUBSTR(INPUT_VALUE, POSITION(':', INPUT_VALUE)+1) AS INTEGER),
           'DAY TO SECOND',    86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(' ', INPUT_VALUE)-1) AS INTEGER) 
                               - 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION(' ', INPUT_VALUE)+1, POSITION(':', INPUT_VALUE)-POSITION(' ', INPUT_VALUE)-1) AS INTEGER)
                               - 60 * CAST(SUBSTR(INPUT_VALUE, POSITION(':', INPUT_VALUE)+1, POSITION(':', INPUT_VALUE, POSITION(':', INPUT_VALUE)+1) - POSITION(':', INPUT_VALUE) - 1) AS INTEGER)
                               - CAST(SUBSTR(INPUT_VALUE,POSITION(':', INPUT_VALUE, POSITION(':', INPUT_VALUE)+1)+1) AS DECIMAL(10,6)),
           'HOUR',             3600 * INPUT_VALUE, 
           'HOUR TO MINUTE',   3600 * CAST(SUBSTR(INPUT_VALUE,1 , POSITION(':', INPUT_VALUE)-1) AS INTEGER)
                               - 60 * CAST(SUBSTR(INPUT_VALUE,POSITION(':', INPUT_VALUE)+1) AS INTEGER),
           'HOUR TO SECOND',   3600 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(':', INPUT_VALUE)-POSITION(' ', INPUT_VALUE)-1) AS INTEGER)
                               - 60 * CAST(SUBSTR(INPUT_VALUE, POSITION(':', INPUT_VALUE)+1, POSITION(':', INPUT_VALUE, POSITION(':', INPUT_VALUE)+1) - POSITION(':', INPUT_VALUE) - 1) AS INTEGER)
                               - CAST(SUBSTR(INPUT_VALUE,POSITION(':', INPUT_VALUE, POSITION(':', INPUT_VALUE)+1)+1) AS DECIMAL(10,6)),  
           'MINUTE',           60 * INPUT_VALUE,     
           'MINUTE TO SECOND', 60 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(':', INPUT_VALUE)-POSITION(' ', INPUT_VALUE)-1) AS INTEGER)
                               - CAST(SUBSTR(INPUT_VALUE, POSITION(':', INPUT_VALUE)+1) AS DECIMAL(10,6)),
           'SECOND',           INPUT_VALUE                                    
            )
ELSE
   DECODE(INPUT_PART,
           'DAY',              86400 * INPUT_VALUE, 
           'DAY TO HOUR',      86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(' ', INPUT_VALUE)-1) AS INTEGER) 
                               + 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION(' ', INPUT_VALUE)+1) AS INTEGER),
           'DAY TO MINUTE',    86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(' ', INPUT_VALUE)-1) AS INTEGER) 
                               + 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION(' ', INPUT_VALUE)+1, POSITION(':', INPUT_VALUE)-POSITION(' ', INPUT_VALUE)-1) AS INTEGER) 
                               + 60 * CAST(SUBSTR(INPUT_VALUE, POSITION(':', INPUT_VALUE)+1) AS INTEGER),
           'DAY TO SECOND',    86400 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(' ', INPUT_VALUE)-1) AS INTEGER) 
                               + 3600 * CAST(SUBSTR(INPUT_VALUE, POSITION(' ', INPUT_VALUE)+1, POSITION(':', INPUT_VALUE)-POSITION(' ', INPUT_VALUE)-1) AS INTEGER)
                               + 60 * CAST(SUBSTR(INPUT_VALUE, POSITION(':', INPUT_VALUE)+1, POSITION(':', INPUT_VALUE, POSITION(':', INPUT_VALUE)+1) - POSITION(':', INPUT_VALUE) - 1) AS INTEGER)
                               + CAST(SUBSTR(INPUT_VALUE,POSITION(':', INPUT_VALUE, POSITION(':', INPUT_VALUE)+1)+1) AS DECIMAL(10,6)),
           'HOUR',             3600 * INPUT_VALUE, 
           'HOUR TO MINUTE',   3600 * CAST(SUBSTR(INPUT_VALUE,1 , POSITION(':', INPUT_VALUE)-1) AS INTEGER)
                               + 60 * CAST(SUBSTR(INPUT_VALUE,POSITION(':', INPUT_VALUE)+1) AS INTEGER),
           'HOUR TO SECOND',   3600 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(':', INPUT_VALUE)-POSITION(' ', INPUT_VALUE)-1) AS INTEGER)
                               + 60 * CAST(SUBSTR(INPUT_VALUE, POSITION(':', INPUT_VALUE)+1, POSITION(':', INPUT_VALUE, POSITION(':', INPUT_VALUE)+1) - POSITION(':', INPUT_VALUE) - 1) AS INTEGER)
                               + CAST(SUBSTR(INPUT_VALUE,POSITION(':', INPUT_VALUE, POSITION(':', INPUT_VALUE)+1)+1) AS DECIMAL(10,6)),  
           'MINUTE',           60 * INPUT_VALUE,     
           'MINUTE TO SECOND', 60 * CAST(SUBSTR(INPUT_VALUE, 1, POSITION(':', INPUT_VALUE)-POSITION(' ', INPUT_VALUE)-1) AS INTEGER)
                               + CAST(SUBSTR(INPUT_VALUE, POSITION(':', INPUT_VALUE)+1) AS DECIMAL(10,6)), 
           'SECOND',           INPUT_VALUE                                    
        )
END
$$;

-- =============================================
-- Description: UDF for converting Seconds to an Interval
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.SECONDS_TO_INTERVAL_UDF
(INPUT_PART VARCHAR(30), INPUT_VALUE NUMBER)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
DECODE(INPUT_PART,
                'DAY',                TRUNC((INPUT_VALUE/(86400)))::varchar,
                'DAY TO HOUR',        TRUNC(INPUT_VALUE/(86400))::varchar || ' ' || 
                                            CASE 
                                                WHEN ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) = 0 THEN '00' 
                                                WHEN ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) > -10 AND ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) < 10 THEN '0' ELSE '' END || 
                                            ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600))::varchar,
                'DAY TO MINUTE',      TRUNC(INPUT_VALUE/(86400))::varchar || ' ' || 
                                            CASE 
                                                WHEN ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) = 0 THEN '00' 
                                                WHEN ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) > -10 AND ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) < 10 THEN '0' ELSE '' END || 
                                            ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600))::varchar || ':' || 
                                                CASE 
                                                    WHEN ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) = 0 THEN '00' 
                                                    WHEN ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) > -10 AND ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) < 10 THEN '0' ELSE '' END || 
                                                ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60))::varchar,
                'DAY TO SECOND',      TRUNC(INPUT_VALUE/(86400))::varchar || ' ' || 
                                            CASE 
                                                WHEN ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) = 0 THEN '00' 
                                                WHEN ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) > -10 AND ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600)) < 10 THEN '0' ELSE '' END || 
                                            ABS(TRUNC(MOD(INPUT_VALUE,86400)/3600))::varchar || ':' || 
                                                CASE 
                                                    WHEN ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) = 0 THEN '00' 
                                                    WHEN ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) > -10 AND ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) < 10 THEN '0' ELSE '' END || 
                                               ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) || ':' ||
                                                    CASE 
                                                        WHEN ABS(MOD(INPUT_VALUE, 60)) = 0 THEN '00' 
                                                        WHEN ABS(MOD(INPUT_VALUE, 60)) > -10 AND ABS(MOD(INPUT_VALUE, 60)) < 10 THEN '0' ELSE '' END || 
                                                    ABS(MOD(INPUT_VALUE, 60))::varchar,
                'HOUR',               TRUNC((INPUT_VALUE/3600))::varchar,     
                'HOUR TO MINUTE',     TRUNC(INPUT_VALUE/3600)::varchar || ':' || 
                                            CASE 
                                                WHEN ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) = 0 THEN '00' 
                                                WHEN ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) > -10 AND ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) < 10 THEN '0' ELSE '' END || 
                                             ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60))::varchar,
                'HOUR TO SECOND',     TRUNC(INPUT_VALUE/3600)::varchar || ':' || 
                                            CASE WHEN ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) = 0 THEN '00' WHEN ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) > -10 AND ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) < 10 THEN '0' ELSE '' END || ABS(TRUNC(MOD(INPUT_VALUE, 3600)/60)) || ':' ||
                                                CASE WHEN ABS(MOD(INPUT_VALUE, 60)) = 0 THEN '00' WHEN ABS(MOD(INPUT_VALUE, 60)) > -10 AND ABS(MOD(INPUT_VALUE, 60)) < 10 THEN '0' ELSE '' END || ABS(MOD(INPUT_VALUE, 60))::varchar,
                'MINUTE',             TRUNC((INPUT_VALUE/60))::varchar,                
                'MINUTE TO SECOND',   TRUNC(INPUT_VALUE/60)::varchar || ':' || 
                                            CASE WHEN ABS(MOD(INPUT_VALUE, 60)) = 0 THEN '00' WHEN ABS(MOD(INPUT_VALUE, 60)) > -10 AND ABS(MOD(INPUT_VALUE, 60)) < 10 THEN '0' ELSE '' END || ABS(MOD(INPUT_VALUE, 60))::varchar,
                'SECOND',             INPUT_VALUE::varchar
)
$$;

-- =============================================
-- Description: UDF for Teradata INTERVAL_MULTIPLY function
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.INTERVAL_MULTIPLY_UDF
(INPUT_PART VARCHAR(30), INPUT_VALUE VARCHAR(), INPUT_MULT INTEGER)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE WHEN INPUT_PART = 'YEAR TO MONTH'
THEN PUBLIC.MONTHS_TO_INTERVAL_UDF(INPUT_PART, PUBLIC.INTERVAL_TO_MONTHS_UDF(INPUT_VALUE) * INPUT_MULT)
ELSE PUBLIC.SECONDS_TO_INTERVAL_UDF(INPUT_PART, PUBLIC.INTERVAL_TO_SECONDS_UDF(INPUT_PART, INPUT_VALUE) * INPUT_MULT)
END
$$;

-- =============================================
-- Description: UDF for Teradata INTERVAL_DIVIDE function
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.INTERVAL_DIVIDE_UDF
(INPUT_PART VARCHAR(30), INPUT_VALUE VARCHAR(), INPUT_DIV INTEGER)
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE WHEN INPUT_PART = 'YEAR TO MONTH'
THEN PUBLIC.MONTHS_TO_INTERVAL_UDF(INPUT_PART, PUBLIC.INTERVAL_TO_MONTHS_UDF(INPUT_VALUE) / INPUT_DIV)
ELSE PUBLIC.SECONDS_TO_INTERVAL_UDF(INPUT_PART, PUBLIC.INTERVAL_TO_SECONDS_UDF(INPUT_PART, INPUT_VALUE) / INPUT_DIV)
END
$$;

-- =============================================
-- Description: UDF for Teradata INTERVAL_ADD function
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.INTERVAL_ADD_UDF
(INPUT_VALUE1 VARCHAR(), INPUT_PART1 VARCHAR(30), INPUT_VALUE2 VARCHAR(), INPUT_PART2 VARCHAR(30), OP CHAR, OUTPUT_PART VARCHAR())
RETURNS VARCHAR
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CASE 
    WHEN INPUT_PART1 = 'YEAR TO MONTH' OR INPUT_PART2 = 'YEAR TO MONTH' THEN
        CASE 
            WHEN OP = '+' THEN
                PUBLIC.SECONDS_TO_INTERVAL_UDF(OUTPUT_PART, PUBLIC.INTERVAL_TO_MONTHS_UDF(INPUT_VALUE1) + PUBLIC.INTERVAL_TO_MONTHS_UDF(INPUT_VALUE2))
            WHEN OP = '-' THEN
                PUBLIC.SECONDS_TO_INTERVAL_UDF(OUTPUT_PART, PUBLIC.INTERVAL_TO_MONTHS_UDF(INPUT_VALUE1) - PUBLIC.INTERVAL_TO_MONTHS_UDF(INPUT_VALUE2))
        END
    ELSE 
        CASE 
            WHEN OP = '+' THEN
                PUBLIC.SECONDS_TO_INTERVAL_UDF(OUTPUT_PART, PUBLIC.INTERVAL_TO_SECONDS_UDF(INPUT_PART1, INPUT_VALUE1) + PUBLIC.INTERVAL_TO_SECONDS_UDF(INPUT_PART2, INPUT_VALUE2))
            WHEN OP = '-' THEN
                PUBLIC.SECONDS_TO_INTERVAL_UDF(OUTPUT_PART, PUBLIC.INTERVAL_TO_SECONDS_UDF(INPUT_PART1, INPUT_VALUE1) - PUBLIC.INTERVAL_TO_SECONDS_UDF(INPUT_PART2, INPUT_VALUE2))
        END  
END
$$;

-- ==============================================================================================================
-- Description: UDF used to add or subtract an interval value to a Date/Timestamp value.
--              Generated when a non-literal interval is used to emulate the Teradata functionality.
-- Parameters:
--              1) DATETIMEVALUE: The date/timestamp that will get their value added/subtracted by the interval.
--              2) INTERVALVALUES: The string representation of the Teradata interval to add/subtract.
--              3) STARTPART: This keyword indicates at which part of DATETIMEVALUE the operation should start,
--                            it is based in the start point of the original interval in Teradata.
--              4) OPERATOR: Indicates whether the operation is an addition (+) or subtraction (-)
-- Example: SELECT PUBLIC.DATETIMEINTERVALADD_UDF(TIMESTAMP '2020-08-13 10:30:25', '3 05:00:10', 'DAY', '+');
-- Result: 2020-08-16 15:30:35.000
-- ==============================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.DATETIMEINTERVALADD_UDF(DATETIMEVALUE TIMESTAMP, INTERVALVALUES STRING, STARTPART STRING, OPERATOR STRING)
RETURNS TIMESTAMP
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    const regexp = /[0-9.]+/g;
    const dateTimeParts = ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'];
    const timestampParts = [DATETIMEVALUE.getFullYear(), DATETIMEVALUE.getMonth(), DATETIMEVALUE.getDate(), DATETIMEVALUE.getHours(), DATETIMEVALUE.getMinutes(), DATETIMEVALUE.getSeconds()]
    const intervalMultiplier = INTERVALVALUES.startsWith('-') ? -1 : 1;
    const numValues = INTERVALVALUES.match(regexp);
    var partIndex = dateTimeParts.indexOf(STARTPART);
    for (let i = 0; i < numValues.length; i++)
    {
        let numValue = parseFloat(numValues[i]);
        if (OPERATOR == '+')
        {
            timestampParts[partIndex] += intervalMultiplier * numValue;
        }
        else
        {
            timestampParts[partIndex] -= intervalMultiplier * numValue;
        }
        partIndex++;
    }

    return new Date(timestampParts[0], timestampParts[1], timestampParts[2], timestampParts[3], timestampParts[4], timestampParts[5]);
$$;

-- ==============================================================================================================
-- Description: UDF used to add an interval value to a Time value.
--              Generated when a non-literal interval is used to emulate the Teradata functionality.
-- Parameters:
--              1) TIMEVALUE: The time that will get their value added by the interval.
--              2) INTERVALVALUES: The string representation of the Teradata interval to add/subtract.
--              3) STARTPART: This keyword indicates at which part of DATETIMEVALUE the operation should start,
--                            it is based in the start point of the original interval in Teradata.
-- Example: SELECT TIMEINTERVALADD_UDF(TIME '10:30:25', '03:10', 'MINUTE');
-- Result: 10:33:35
-- ==============================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.TIMEINTERVALADD_UDF(TIMEVALUE TIME, INTERVALVALUES STRING, STARTPART STRING)
RETURNS TIME
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DECODE(
    STARTPART,
    'HOUR', TIME_FROM_PARTS(HOUR(TIMEVALUE) + REGEXP_SUBSTR(INTERVALVALUES,'[0-9.]+', 1, 1),
            MINUTE(TIMEVALUE) + NVL(REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 2), 0),
            SECOND(TIMEVALUE) + NVL(REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 3), 0)),
    'MINUTE', TIME_FROM_PARTS(HOUR(TIMEVALUE),
            MINUTE(TIMEVALUE) + REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 1),
            SECOND(TIMEVALUE) + NVL(REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 2), 0)),
    'SECOND', TIME_FROM_PARTS(HOUR(TIMEVALUE),
            MINUTE(TIMEVALUE),
            SECOND(TIMEVALUE) + REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 1))
    )
$$;

-- ==============================================================================================================
-- Description: UDF used to subtract an interval value to a Time value.
--              Generated when a non-literal interval is used to emulate the Teradata functionality.
-- Parameters:
--              1) TIMEVALUE: The time that will get their value subtracted by the interval.
--              2) INTERVALVALUES: The string representation of the Teradata interval to add/subtract.
--              3) STARTPART: This keyword indicates at which part of DATETIMEVALUE the operation should start,
--                            it is based in the start point of the original interval in Teradata.
-- Example: SELECT TIMEINTERVALSUBTRACT_UDF(TIME '10:30:25', '03:10', 'MINUTE');
-- Result: 10:27:15
-- ==============================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.TIMEINTERVALSUBTRACT_UDF(TIMEVALUE TIME, INTERVALVALUES STRING, STARTPART STRING)
RETURNS TIME
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    DECODE(
    STARTPART,
    'HOUR', TIME_FROM_PARTS(HOUR(TIMEVALUE) - REGEXP_SUBSTR(INTERVALVALUES,'[0-9.]+', 1, 1),
            MINUTE(TIMEVALUE) - NVL(REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 2), 0),
            SECOND(TIMEVALUE) - NVL(REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 3), 0)),
    'MINUTE', TIME_FROM_PARTS(HOUR(TIMEVALUE),
            MINUTE(TIMEVALUE) - REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 1),
            SECOND(TIMEVALUE) - NVL(REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 2), 0)),
    'SECOND', TIME_FROM_PARTS(HOUR(TIMEVALUE),
            MINUTE(TIMEVALUE),
            SECOND(TIMEVALUE) - REGEXP_SUBSTR(INTERVALVALUES, '[0-9.]+', 1, 1))
    )
$$;

-- ===========================================================================================================================
-- Description: UDF used to transform an interval string representation to another interval range.
--              Generated when an interval type field is casted to another interval type to replicate Teradata functionality.
-- Parameters:
--              1) INTERVALVALUE: The string representation of the Teradata interval that will be converted.
--              2) SOURCETYPESTART: The keyword indicating where the original value type starts.
--              3) SOURCETYPEEND: The keyword indicating where the original value type ends.
--              3) TARGETTYPESTART: The keyword indicating where the target interval type starts.
--              3) TARGETTYPEEND: The keyword indicating where the target interval type ends.
-- Example: SELECT PUBLIC.INTERVALTOINTERVAL_UDF('30', 'MINUTE', 'MINUTE', 'DAY', 'MINUTE');
--          SELECT PUBLIC.INTERVALTOINTERVAL_UDF('5 03', 'DAY', 'HOUR', 'HOUR', 'SECOND');
-- Result: 0 00:30
--         123:00:00
-- ===========================================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.INTERVALTOINTERVAL_UDF(INTERVALVALUE STRING, SOURCETYPESTART STRING, SOURCETYPEEND STRING, TARGETTYPESTART STRING, TARGETTYPEEND STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
    const regexp = /[0-9.]+/g;
    const dateTimeParts = ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'];
    const partValues = [0, 0, 0, 0, 0, 0];
    const nextPartMultiplier = [12, 30, 24, 60, 60, 1];
    const partDivisors = [1, 12, 30, 24, 60, 60];
    const nextPartSeparator = ['', '-', ' ', ' ', ':', ':'];
    const numValues = INTERVALVALUE.match(regexp);
    const sourceStartIndex = dateTimeParts.indexOf(SOURCETYPESTART);
    const targetStartIndex = dateTimeParts.indexOf(TARGETTYPESTART);
    const sourceEndIndex = dateTimeParts.indexOf(SOURCETYPEEND);
    const targetEndIndex = dateTimeParts.indexOf(TARGETTYPEEND);
    var multiplier = 1;
    var accumulator = 0;
    var numValuesPos = numValues.length - 1;
    var result = INTERVALVALUE.startsWith('-') ? '-' : '';
    for (let i = sourceEndIndex; i >= sourceStartIndex; i--)
    {
        partValues[i] = Number(numValues[numValuesPos--]);
        if (i < targetStartIndex)
        {
            multiplier *= nextPartMultiplier[i];
            partValues[targetStartIndex] += partValues[i] * multiplier; 
        }
    }

    for (let i = 5; i > targetStartIndex; i--)
    {
        let divisor = partDivisors[i];
        let partValue = partValues[i] + accumulator;
        partValues[i] = partValue % divisor;
        accumulator = Math.floor(partValue / divisor);
    }

    if (accumulator != 0)
    {
        partValues[targetStartIndex] += accumulator;
    }


    for (let i = targetStartIndex; i <= targetEndIndex; i++)
    {
        let separator = i == targetStartIndex ? '' : nextPartSeparator[i];
        let partValue = partValues[i].toString();
        let resultValue = i == targetStartIndex ? partValue : partValue.padStart(2, '0');
        result = result.concat(separator, resultValue);
    }

    return result;
$$;