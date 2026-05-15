-- <copyright file="ROUND_DATE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =============================================
-- DESCRIPTION: RETURNS DATE_VALUE WITH THE TIME PORTION OF THE DAY ROUNDED TO THE UNIT SPECIFIED BY UNIT_TO_ROUND_BY.
-- EQUIVALENT: TERADATA'S ROUND(Date) FUNCTIONALITY
-- PARAMETERS:
-- DATE_TO_ROUND: TIMESTAMP_LTZ
-- UNIT_TO_ROUND_BY: VARCHAR
-- RETURNS: DATE ROUNDED BY THE UNIT DEFINED IN UNIT_TO_ROUND_BY
-- =============================================
CREATE OR REPLACE FUNCTION PUBLIC.ROUND_DATE_UDF(DATE_TO_ROUND TIMESTAMP_LTZ, UNIT_TO_ROUND_BY VARCHAR(5))
RETURNS DATE
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
CAST(
CASE 
    WHEN UNIT_TO_ROUND_BY IN ('CC','SCC') THEN 
        CASE 
            WHEN RIGHT(CAST(YEAR(DATE_TO_ROUND) as CHAR(4)),2) >=51 
                THEN DATE_FROM_PARTS(CAST(LEFT(CAST(YEAR(DATE_TO_ROUND) AS CHAR(4)),2) || '01' AS INTEGER) +100,1,1)
            ELSE DATE_FROM_PARTS(CAST(LEFT(CAST(YEAR(DATE_TO_ROUND) AS CHAR(4)),2) || '01' AS INTEGER),1,1)
        END    
    WHEN UNIT_TO_ROUND_BY IN ('SYYYY','YYYY','YEAR','SYEAR','YYY','YY','Y') THEN 
        CASE WHEN MONTH(DATE_TO_ROUND) >= 7 THEN DATE_FROM_PARTS(YEAR(DATE_TO_ROUND)+1,1,1)
             ELSE DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1)
        END
    WHEN UNIT_TO_ROUND_BY IN ('IYYY','IYY','IY','I') THEN 
        CASE WHEN MONTH(DATE_TO_ROUND) >= 7 THEN CASE DAYOFWEEK(DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),12,31))
                                                                  WHEN 0 THEN DATEADD(DAY, 1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),12,31))
                                                                  WHEN 1 THEN DATEADD(DAY, 0, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),12,31))
                                                                  WHEN 2 THEN DATEADD(DAY, -1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),12,31))
                                                                  WHEN 3 THEN DATEADD(DAY, -2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),12,31))
                                                                  WHEN 4 THEN DATEADD(DAY, -3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),12,31))
                                                                  WHEN 5 THEN DATEADD(DAY, 3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),12,31))
                                                                  WHEN 6 THEN DATEADD(DAY, 2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),12,31))
                                                              END
             ELSE CASE DAYOFWEEK(DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                      WHEN 0 THEN DATEADD(DAY, 1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                      WHEN 1 THEN DATEADD(DAY, 0, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                      WHEN 2 THEN DATEADD(DAY, -1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                      WHEN 3 THEN DATEADD(DAY, -2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                      WHEN 4 THEN DATEADD(DAY, -3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                      WHEN 5 THEN DATEADD(DAY, 3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                      WHEN 6 THEN DATEADD(DAY, 2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                  END  
        END
    WHEN UNIT_TO_ROUND_BY IN ('MONTH','MON','MM','RM') THEN 
        CASE WHEN DAYOFMONTH(DATE_TO_ROUND) >15 THEN TIMESTAMPADD(MONTH, 1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),MONTH(DATE_TO_ROUND),1))
             ELSE DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),MONTH(DATE_TO_ROUND),1)
        END
    WHEN UNIT_TO_ROUND_BY IN ('Q') THEN 
        CASE WHEN (MOD(MONTH(DATE_TO_ROUND),3)=2 AND DAYOFMONTH(DATE_TO_ROUND) >15) OR MOD(MONTH(DATE_TO_ROUND),3)=0 
                THEN TIMESTAMPADD(MONTH, 3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),(QUARTER(DATE_TO_ROUND)-1)*3+1,1)) 
             ELSE DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),(QUARTER(DATE_TO_ROUND)-1)*3+1,1)
        END
    WHEN UNIT_TO_ROUND_BY IN ('WW') THEN 
        CASE WHEN MOD(TIMESTAMPDIFF(DAY,DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1),DATE_TO_ROUND),7) < 4 
                THEN DATEADD(DAY, 0-MOD(TIMESTAMPDIFF(DAY,DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1),DATE_TO_ROUND),7), DATE_TO_ROUND)
             ELSE DATEADD(DAY, 7-MOD(TIMESTAMPDIFF(DAY,DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1),DATE_TO_ROUND),7), DATE_TO_ROUND)
        END
    WHEN UNIT_TO_ROUND_BY IN ('IW') THEN 
        CASE WHEN MOD(TIMESTAMPDIFF(DAY,(CASE DAYOFWEEK(DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                             WHEN 0 THEN DATEADD(DAY, 1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                             WHEN 1 THEN DATEADD(DAY, 0, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                             WHEN 2 THEN DATEADD(DAY, -1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                             WHEN 3 THEN DATEADD(DAY, -2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                             WHEN 4 THEN DATEADD(DAY, -3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                             WHEN 5 THEN DATEADD(DAY, 3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                             WHEN 6 THEN DATEADD(DAY, 2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                         END
                   ), DATE_TO_ROUND),7) >=4 THEN DATEADD(DAY,7-MOD(TIMESTAMPDIFF(DAY,(CASE DAYOFWEEK(DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                                                    WHEN 0 THEN DATEADD(DAY, 1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                                                    WHEN 1 THEN DATEADD(DAY, 0, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                                                    WHEN 2 THEN DATEADD(DAY, -1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                                                    WHEN 3 THEN DATEADD(DAY, -2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                                                    WHEN 4 THEN DATEADD(DAY, -3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                                                    WHEN 5 THEN DATEADD(DAY, 3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                                                    WHEN 6 THEN DATEADD(DAY, 2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                                                END), DATE_TO_ROUND),7), DATE_TO_ROUND)
             ELSE DATEADD(DAY,0-MOD(TIMESTAMPDIFF(DAY,(CASE DAYOFWEEK(DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                           WHEN 0 THEN DATEADD(DAY, 1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                           WHEN 1 THEN DATEADD(DAY, 0, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                           WHEN 2 THEN DATEADD(DAY, -1, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                           WHEN 3 THEN DATEADD(DAY, -2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                           WHEN 4 THEN DATEADD(DAY, -3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                           WHEN 5 THEN DATEADD(DAY, 3, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                           WHEN 6 THEN DATEADD(DAY, 2, DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),1,1))
                                                       END), DATE_TO_ROUND),7), DATE_TO_ROUND)
        END
    WHEN UNIT_TO_ROUND_BY IN ('W') THEN 
        CASE WHEN MOD(TIMESTAMPDIFF(DAY,DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),MONTH(DATE_TO_ROUND),1),DATE_TO_ROUND),7) < 4 
                THEN DATEADD(DAY, 0-MOD(TIMESTAMPDIFF(DAY,DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),MONTH(DATE_TO_ROUND),1),DATE_TO_ROUND),7), DATE_TO_ROUND)
            ELSE DATEADD(DAY, 7-MOD(TIMESTAMPDIFF(DAY,DATE_FROM_PARTS(YEAR(DATE_TO_ROUND),MONTH(DATE_TO_ROUND),1),DATE_TO_ROUND),7), DATE_TO_ROUND)
        END
    WHEN UNIT_TO_ROUND_BY IN ('DDD', 'DD','J') THEN DATE_TO_ROUND
    WHEN UNIT_TO_ROUND_BY IN ('DAY', 'DY','D') THEN 
        CASE WHEN DAYOFWEEK(DATE_TO_ROUND) > 3 THEN DATEADD(DAY, 7-DAYOFWEEK(DATE_TO_ROUND), DATE_TO_ROUND)
             ELSE DATEADD(DAY, 0-DAYOFWEEK(DATE_TO_ROUND), DATE_TO_ROUND)
        END
    WHEN UNIT_TO_ROUND_BY IN ('HH', 'HH12','HH24') THEN DATE_TO_ROUND
    WHEN UNIT_TO_ROUND_BY IN ('MI') THEN DATE_TO_ROUND
END AS DATE)
$$;