-- <copyright file="LOG_INFO_UDP.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- =========================================================================================================
-- Procedure: LOG_INFO_UDP
-- Description: Logs an informational message using the Snowflake system logging function
--              and returns a success message.
-- Parameters:
--   MESSAGE (VARCHAR): The message to be logged.
-- Returns:
--   VARCHAR: A success message indicating the log operation was completed.
-- =========================================================================================================
CREATE OR REPLACE PROCEDURE PUBLIC.LOG_INFO_UDP(MESSAGE VARCHAR)
 RETURNS VARCHAR
 LANGUAGE SQL
<SnowConvertVersionComment>
 EXECUTE AS CALLER
 AS
 $$
    BEGIN
        SYSTEM$LOG_INFO(MESSAGE);
        RETURN 'Message logged successfully';
    END;
 $$;