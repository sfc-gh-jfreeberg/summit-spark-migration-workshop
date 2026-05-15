-- <copyright file="PERIOD_OVERLAPS_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================
-- DESCRIPTION: UDF that reproduces the OVERLAPS OPERATOR to compare two or more  
--      period expressions and indicate if all expressions overlap with each other.
-- PARAMETERS:
--      PERIODS: ARRAY all the period expressions to be compared.
-- RETURNS:
--      TRUE if all period expressions overlap, otherwise FALSE.
-- ======================================================================
CREATE OR REPLACE FUNCTION PUBLIC.PERIOD_OVERLAPS_UDF(PERIODS ARRAY)
RETURNS BOOLEAN
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
        return false;
    }
    
    var currentPeriod = splitPeriod(PERIODS[0]);
    for (var i = 1; i < PERIODS.length; i++) {
        var nextPeriod = splitPeriod(PERIODS[i]);
        if(!overlaps(currentPeriod, nextPeriod)) {
            return false;
        }
        currentPeriod = [greatest(currentPeriod[0], nextPeriod[0]), least(currentPeriod[1], nextPeriod[1])];
    }
    
    return true;

} catch (error) {
    return false;
}
$$;