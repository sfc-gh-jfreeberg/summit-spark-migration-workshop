-- <copyright file="UTL_FILE_FOPEN_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ================================================================================
-- Description: The UTL_FILE_FOPEN procedure is used 
-- to replicate the functionality of Oracle UTL_FILE_FOPEN procedure.
-- ================================================================================

CREATE SCHEMA IF NOT EXISTS UTL_FILE
<SnowConvertVersionComment>;

CREATE OR REPLACE PROCEDURE UTL_FILE.FOPEN_UDP(FILENAME VARCHAR,OPEN_MODE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
<SnowConvertVersionComment>
AS $$
    DECLARE
        fhandle VARCHAR;
        key VARCHAR;
        status VARCHAR;
        File_is_not_loaded_on_stage exception;
        fileNameConcat VARCHAR:= '@UTL_FILE.utlfile_local_directory/'||:FILENAME;
        copyIntoQuery VARCHAR DEFAULT 'COPY INTO UTL_FILE.FOPEN_TABLES_LINES (FHANDLE, LINE) FROM (SELECT ? , stageFile.$1 FROM '||:fileNameConcat||' stageFile)';
    BEGIN
        CREATE TABLE IF NOT EXISTS UTL_FILE.FOPEN_TABLES 
        (
          FHANDLE VARCHAR, 
          FILENAME VARCHAR,
          OPEN_MODE VARCHAR
        );

        CREATE TABLE IF NOT EXISTS UTL_FILE.FOPEN_TABLES_LINES 
        (
          SEQ    NUMBER AUTOINCREMENT,
          FHANDLE VARCHAR, 
          LINE    VARCHAR
        );   
        SELECT FHANDLE INTO fhandle FROM UTL_FILE.FOPEN_TABLES WHERE FILENAME = :FILENAME;
        SELECT UUID_STRING() INTO key;
        IF (OPEN_MODE = 'w') THEN
            INSERT INTO UTL_FILE.FOPEN_TABLES(FHANDLE, FILENAME, OPEN_MODE) VALUES(:key,:FILENAME,:OPEN_MODE);
            RETURN TO_JSON({ 'name': FILENAME, 'handle': key});
        ELSE
            IF (fhandle IS NULL) THEN
                EXECUTE IMMEDIATE :copyIntoQuery USING (key);
                SELECT OBJECT_CONSTRUCT(*):status INTO status FROM table(result_scan(last_query_id()));
                IF (status = 'LOADED') THEN
                    INSERT INTO UTL_FILE.FOPEN_TABLES(FHANDLE, FILENAME, OPEN_MODE) VALUES(:key,:FILENAME,:OPEN_MODE);
                    RETURN TO_JSON({'name': FILENAME, 'handle': key});
                ELSE
                    raise File_is_not_loaded_on_stage;
                END IF;
            ELSE
                UPDATE UTL_FILE.FOPEN_TABLES SET OPEN_MODE = :OPEN_MODE WHERE FHANDLE = :fhandle;
                RETURN TO_JSON({'name': FILENAME, 'handle': fhandle});
           END IF;
        END IF;     
    END
$$;

CREATE OR REPLACE PROCEDURE UTL_FILE.FOPEN_UDP(PACKAGE_VARIABLE VARCHAR, FILENAME VARCHAR, OPEN_MODE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
<SnowConvertVersionComment>
AS $$
    DECLARE
        fhandle VARCHAR;
        key VARCHAR;
        status VARCHAR;
        result VARCHAR;
        File_is_not_loaded_on_stage exception;
        fileNameConcat VARCHAR:= '@UTL_FILE.utlfile_local_directory/'||:FILENAME;
        copyIntoQuery VARCHAR DEFAULT 'COPY INTO UTL_FILE.FOPEN_TABLES_LINES (FHANDLE, LINE) FROM (SELECT ? , stageFile.$1 FROM '||:fileNameConcat||' stageFile)';
    BEGIN
        CREATE TABLE IF NOT EXISTS UTL_FILE.FOPEN_TABLES 
        (
          FHANDLE VARCHAR, 
          FILENAME VARCHAR,
          OPEN_MODE VARCHAR
        );

        CREATE TABLE IF NOT EXISTS UTL_FILE.FOPEN_TABLES_LINES 
        (
          SEQ    NUMBER AUTOINCREMENT,
          FHANDLE VARCHAR, 
          LINE    VARCHAR
        );   
        SELECT FHANDLE INTO fhandle FROM UTL_FILE.FOPEN_TABLES WHERE FILENAME = :FILENAME;
        SELECT UUID_STRING() INTO key;
        IF (OPEN_MODE = 'w') THEN
            INSERT INTO UTL_FILE.FOPEN_TABLES(FHANDLE, FILENAME, OPEN_MODE) VALUES(:key,:FILENAME,:OPEN_MODE);
            result := TO_JSON({'name': FILENAME, 'handle': key});
            CALL UPDATE_PACKAGE_VARIABLE_STATE_UDP(:PACKAGE_VARIABLE, TO_VARCHAR(:result));
            RETURN result;
        ELSE
            IF (fhandle IS NULL) THEN
                EXECUTE IMMEDIATE :copyIntoQuery USING (key);
                SELECT OBJECT_CONSTRUCT(*):status INTO status FROM table(result_scan(last_query_id()));
                IF (status = 'LOADED') THEN
                    INSERT INTO UTL_FILE.FOPEN_TABLES(FHANDLE, FILENAME, OPEN_MODE) VALUES(:key,:FILENAME,:OPEN_MODE);
                    result := TO_JSON({'name': FILENAME, 'handle': key});
                    CALL UPDATE_PACKAGE_VARIABLE_STATE_UDP(:PACKAGE_VARIABLE, TO_VARCHAR(:result));
                    RETURN result;
                ELSE
                    raise File_is_not_loaded_on_stage;
                END IF;
            ELSE
                UPDATE UTL_FILE.FOPEN_TABLES SET OPEN_MODE = :OPEN_MODE WHERE FHANDLE = :fhandle;
                result := TO_JSON({'name': FILENAME, 'handle': fhandle});
                CALL UPDATE_PACKAGE_VARIABLE_STATE_UDP(:PACKAGE_VARIABLE, TO_VARCHAR(:result));
                RETURN result;
           END IF;
        END IF;     
    END
$$;