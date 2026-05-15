-- <copyright file="UTL_FILE_PUT_LINE_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ================================================================================
-- Description: The UTL_FILE_PUT_LINE procedure is used 
-- to replicate the functionality of Oracle UTL_FILE_PUT_LINE procedure.
-- ================================================================================

CREATE SCHEMA IF NOT EXISTS UTL_FILE
<SnowConvertVersionComment>;

CREATE OR REPLACE PROCEDURE UTL_FILE.PUT_LINE_UDP(FILE VARCHAR,BUFFER VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
<SnowConvertVersionComment>
AS $$
    DECLARE 
        openMode VARCHAR;
        openModeTemp VARCHAR;
        fhandle VARCHAR;
        fileParse VARIANT;
        File_is_read_only exception;
    BEGIN
        fileParse:= PARSE_JSON(FILE);
        fhandle:= :fileParse:handle;
        SELECT OPEN_MODE INTO openModeTemp FROM UTL_FILE.FOPEN_TABLES WHERE FHANDLE = :fhandle; 
        IF (openModeTemp = 'a' or openModeTemp = 'w') THEN
            INSERT INTO UTL_FILE.FOPEN_TABLES_LINES(FHANDLE,LINE) VALUES(:fhandle,:BUFFER);
        ELSE  
            raise File_is_read_only;
        END IF;
    END
$$;  
    