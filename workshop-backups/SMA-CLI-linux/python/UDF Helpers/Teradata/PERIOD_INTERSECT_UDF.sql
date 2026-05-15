-- <copyright file="PERIOD_INTERSECT_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================
-- DESCRIPTION: UDF that reproduces the P_INTERSECT OPERATOR to compare two or more  
--      period expressions and return the overlapping portion of the period expressions.
-- PARAMETERS:
--      PERIODS: ARRAY all the period expressions to be compared.
-- RETURNS:
--      The overlapping portion of the period expressions.
-- ======================================================================
CREATE OR REPLACE FUNCTION PUBLIC.PERIOD_INTERSECT_UDF(PERIODS ARRAY)
RETURNS STRING
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
function overlaps(period1, period2) {
    return !(period1[0] >= period2[1] || period2[0] >= period1[1]);
}

function splitPeriod(period){
    return period.split('*');
}

function greatest(date1, date2){
    return date1 > date2 ? date1 : date2;
}

function least(date1, date2){
    return date1 < date2 ? date1 : date2;
}

try {
    if ((PERIODS.includes(null))) {
        return null;
    }
    
    var currentPeriod = splitPeriod(PERIODS[0]);
    for (var i = 1; i < PERIODS.length; i++) {
        var nextPeriod = splitPeriod(PERIODS[i]);
        if(!overlaps(currentPeriod, nextPeriod)) {
            return null;
        }
        currentPeriod = [greatest(currentPeriod[0], nextPeriod[0]), least(currentPeriod[1], nextPeriod[1])];
    }
    
    return currentPeriod[0] + '*' +  currentPeriod[1];

} catch (error) {
    return null;
}
$$;