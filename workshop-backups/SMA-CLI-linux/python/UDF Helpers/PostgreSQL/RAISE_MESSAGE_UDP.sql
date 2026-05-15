-- <copyright file="RAISE_MESSAGE_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

CREATE OR REPLACE PROCEDURE PUBLIC.RAISE_MESSAGE_UDP(LEVEL VARCHAR, MESSAGE VARCHAR, ARGS VARIANT DEFAULT ARRAY_CONSTRUCT())
RETURNS VARCHAR
LANGUAGE SQL
<SnowConvertVersionComment>
EXECUTE AS CALLER
AS
$$
    DECLARE
        MY_EXCEPTION EXCEPTION (-20002, 'To view the EXCEPTION MESSAGE, you need to check the log.');
        SC_RAISE_MESSAGE_UDP VARCHAR;
    BEGIN
        SC_RAISE_MESSAGE_UDP := PUBLIC.STRING_FORMAT_UDF(MESSAGE, ARGS);
        IF (LEVEL = 'EXCEPTION') THEN
            SYSTEM$LOG_ERROR(SC_RAISE_MESSAGE_UDP);
            RAISE MY_EXCEPTION;
        ELSEIF (LEVEL = 'WARNING') THEN
            SYSTEM$LOG_WARN(SC_RAISE_MESSAGE_UDP);
            RETURN 'Warning printed successfully';
        ELSE
            SYSTEM$LOG_INFO(SC_RAISE_MESSAGE_UDP);
            RETURN 'Message printed successfully';
        END IF;
    END;
$$;
