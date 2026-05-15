-- <copyright file="UTL_FILE_FCLOSE_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ================================================================================
-- Description: The UTL_FILE_FCLOSE procedure is used 
-- to replicate the functionality of Oracle UTL_FILE_FCLOSE procedure.
-- ================================================================================

CREATE SCHEMA IF NOT EXISTS UTL_FILE
<SnowConvertVersionComment>;

CREATE OR REPLACE PROCEDURE UTL_FILE.FCLOSE_UDP(FILE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
<SnowConvertVersionComment>
AS $$
     DECLARE
        fhandle VARCHAR;
        fileParse VARIANT;
        File_is_read_only exception;
        fileNameConcat VARCHAR;
        copyIntoQuery VARCHAR ;
    BEGIN
        fileParse:= PARSE_JSON(FILE);
        fhandle:= :fileParse:handle;
        fileNameConcat:= '@UTL_FILE.utlfile_local_directory/'||:fileParse:name;
        copyIntoQuery:= 'COPY INTO '||:fileNameConcat||' FROM (SELECT LINE FROM UTL_FILE.FOPEN_TABLES_LINES WHERE FHANDLE = ? ORDER BY SEQ) FILE_FORMAT= (FORMAT_NAME = my_csv_format COMPRESSION=NONE)   OVERWRITE=TRUE';
        EXECUTE IMMEDIATE :copyIntoQuery USING (fhandle);
        DELETE FROM UTL_FILE.FOPEN_TABLES_LINES WHERE FHANDLE = :fhandle;
        DELETE FROM UTL_FILE.FOPEN_TABLES WHERE FHANDLE = :fhandle;
    END
$$;