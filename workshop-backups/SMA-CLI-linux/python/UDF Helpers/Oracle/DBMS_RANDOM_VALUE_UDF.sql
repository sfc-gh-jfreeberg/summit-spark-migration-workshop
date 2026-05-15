-- <copyright file="DBMS_RANDOM_VALUE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ================================================================================
-- Description: The DBMS_RANDOM_VALUE function is used 
-- to replicate the functionality of Oracle DBMS_RANDOM.VALUE function.
-- ================================================================================

CREATE SCHEMA IF NOT EXISTS DBMS_RANDOM
<SnowConvertVersionComment>;

CREATE OR REPLACE FUNCTION DBMS_RANDOM.VALUE_UDF()
RETURNS DOUBLE
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
AS
$$  
  return Math.random();
$$;

CREATE OR REPLACE FUNCTION DBMS_RANDOM.VALUE_UDF(low double, high double)
RETURNS DOUBLE
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
AS
$$
    if (LOW > HIGH) {
        [LOW, HIGH] = [HIGH, LOW];
    }
    
    const MAX_DECIMAL_DIGITS = 38;
    return (Math.random() * (HIGH - LOW) + LOW).toFixed(MAX_DECIMAL_DIGITS);
$$;