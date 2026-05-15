-- <copyright file="JULIAN_TO_GREGORIAN_DATE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- DESCRIPTION: UDF THAT TRANSFORMS A JULIAN DATE INTO FORMATS: JD EDWARDS, YYYYDDD ASTRONOMY AND YYYYDDD ORDINAL
-- EQUIVALENT: 
-- PARAMETERS:
-- JULAN DATE: CHAR - THE JULIAN DATE TO TRANSFORM
-- FORMAT_SELECTED: FORMAT SELECTED
-- RETURNS: THE DATE REPRESENTATION OF THE JULIAN DATE
-- EXAMPLE (JD EDWARDS FORMAT):
--  SELECT JULIAN_TO_GREGORIAN_DATE_UDF('098185');
--  RETURNS '1998-07-04' (a.k.a Sat Jul 04 1998)
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.JULIAN_TO_GREGORIAN_DATE_UDF(JULIAN_DATE CHAR(7), FORMAT_SELECTED CHAR(1))
RETURNS variant
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
AS
$$
    const CONST_FOR_MODIFIED_JULIAN_DATE = 0.5;
    const BEGINNING_OF_GREG_CALENTAR = 2299161;
    const CONST_AFTER_GREG_VALUE = 1867216.25;
    const DIVIDENT_TO_GET_CENTURY = 36524.25;
    const LEAP_YEAR_CONSTANT = 4;
    const CONST_TO_GET_DAY_OF_MONTH = 30.6001;

    //Functions definitions

    function julianToGregorian(julianDate){
        const JD = julianDate + CONST_FOR_MODIFIED_JULIAN_DATE; //setting modified julian date 
        const Z = Math.floor(JD); //setting fractional part of julian day
        const F = JD - Z; //fractional part of the julian date
        let A, alpha, B, C, D, E, year, month, day;
        
        //verification for the beginning of gregorian calendar
        if(Z < BEGINNING_OF_GREG_CALENTAR){ 
            A=Z; 
        } else {
            //alpha is for dates after the beginning of gregorian calendar
            alpha = Math.floor((Z-CONST_AFTER_GREG_VALUE) / DIVIDENT_TO_GET_CENTURY);
            A=Z+1+alpha - Math.floor(alpha/LEAP_YEAR_CONSTANT);
        }

        B = A + 1524;
        C = Math.floor((B-122.1)/365.25);
        D = Math.floor(365.25*C);
        E = Math.floor((B-D)/CONST_TO_GET_DAY_OF_MONTH);

        day= Math.floor(B-D-Math.floor(CONST_TO_GET_DAY_OF_MONTH*E)+F);
        month=(E<14)? E -1: E-13;
        year=(month>2)? C-4716: C-4715;

        return new Date(year, month-1, day);
    }

function cyydddToGregorian(julianDate){
        var c=Math.floor(julianDate/1000);
        var yy=(c<80)? c+2000: c+1900;
        var ddd=julianDate%1000;
        var date= new Date(yy, 0);
        date.setDate(ddd);
        return date;
    }

function ordinalDate(ordinalDate){
    const year = parseInt(ordinalDate.toString().substring(0,4));
    const dayOfYear = parseInt(ordinalDate.toString().substring(4));
    const date = new Date(year, 0); //Set date to the first day of year
    date.setDate(dayOfYear);
    return date;
}

function formatDate(toFormatDate){
    toFormatDate = toFormatDate.toDateString();
    let year = toFormatDate.split(" ")[3];
    let month = toFormatDate.split(" ")[1];
    let day = toFormatDate.split(" ")[2];
    return new Date(month + day + ", " + Math.abs(year)).toISOString().split('T')[0]
}

    switch(FORMAT_SELECTED){
        case 'E': 
            //JD Edwards World formar, century added  - CYYDDD
            var result = formatDate(cyydddToGregorian(parseInt(JULIAN_DATE)));
            return result;
        break; 
        case 'J':
            //astronomical format YYYYDDD
            return formatDate(julianToGregorian(parseInt(JULIAN_DATE)));
        break;
        case 'R':
            //ordinal date format YYYYDDD
            return formatDate(ordinalDate(parseInt(JULIAN_DATE)));
        break;
        default: return null;
    }
    
$$
;
