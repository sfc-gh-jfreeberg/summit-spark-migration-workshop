-- <copyright file="TIMESTAMP_DIFF_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Description: The TIMESTAMP_DIFF_UDF() is used to achieve the timestamps arithmetic operations
-- functional equivalence in Snowflake.
-- =========================================================================================================
CREATE OR REPLACE FUNCTION PUBLIC.TIMESTAMP_DIFF_UDF(LEFT_TS TIMESTAMP, RIGHT_TS TIMESTAMP )
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
WITH RESULTS(days,hours,min,sec,millisecond,sign) AS
(
  SELECT
  abs(TRUNC(x/1000/3600/24)) days,
  abs(TRUNC(x/1000/60 / 60)-trunc(x/1000/3600/24)*24) hours,
  abs(TRUNC(MOD(x/1000,3600)/60)) min,
  abs(TRUNC(MOD(x/1000,60))) sec,
  abs(TRUNC(MOD(x,1000))) millisecond,
  SIGN(x)
  FROM (SELECT TIMESTAMPDIFF(millisecond, RIGHT_TS, LEFT_TS) x ,SIGN(TIMESTAMPDIFF(millisecond, RIGHT_TS, LEFT_TS)) sign))
  SELECT
  IFF(SIGN>0,'+','-') || TRIM(TO_CHAR(days,'000000000')) || ' ' || TO_CHAR(hours,'00') || ':' || TRIM(TO_CHAR(min,'00')) || ':' || TRIM(TO_CHAR(sec,'00')) || '.' || TRIM(TO_CHAR(millisecond,'00000000'))
  from RESULTS
$$;