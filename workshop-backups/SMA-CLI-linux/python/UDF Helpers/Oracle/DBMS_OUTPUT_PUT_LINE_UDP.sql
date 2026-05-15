-- <copyright file="DBMS_OUTPUT_PUT_LINE_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ================================================================================
-- Description: The DBMS_OUTPUT_PUT_LINE procedure is used 
-- to replicate the functionality of Oracle DBMS_OUTPUT_PUT_LINE function.
-- ================================================================================

CREATE SCHEMA IF NOT EXISTS DBMS_OUTPUT
<SnowConvertVersionComment>;

CREATE OR REPLACE procedure DBMS_OUTPUT.PUT_LINE_UDP(LOG VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
<SnowConvertVersionComment>
AS $$
   
  //Performance may be affected by using this UDF.
  //If you want to start logging information, please uncomment the implementation.
  //Once the calls of DBMS_OUTPUT.PUT_LINE have been done, please use
  //the following query to read all the logs:
  //SELECT * FROM DBMS_OUTPUT.DBMS_OUTPUT_LOG.

  //snowflake.execute({sqlText:`
  //CREATE TEMPORARY TABLE IF NOT EXISTS DBMS_OUTPUT_LOG 
  //(
  //  WHEN TIMESTAMP,
  //  DATABASE VARCHAR,
  //  LOG VARCHAR
  //);`});

  //snowflake.execute({sqlText:`INSERT INTO DBMS_OUTPUT_LOG(WHEN, DATABASE, LOG) VALUES (CURRENT_TIMESTAMP,CURRENT_DATABASE(),?)`, binds:[LOG]});
  return LOG;
$$;